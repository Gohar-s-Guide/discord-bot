import asyncio
import datetime
import json
import os
from collections import deque
from typing import Deque, Dict, List, Optional
import storage

import discord
from discord.ext import commands, tasks

# persistence moved to SQLite (storage.py)


class StudyPartner(commands.Cog):
    """Find study partners by queueing users, creating temporary voice+text channels,
    pinging both users when paired, and logging the text channel messages on close.

    Features:
    - `findpartner` (no args): join the queue; if someone waiting, pair them.
    - creates a private voice + text channel for the pair and pings them in the invoking channel
    - `close`: closes the pair channels, posts a transcript to `#findpartner-logs` and deletes the channels
    - automatic closing when the voice channel is empty for a configurable timeout
    """

    AUTO_CLOSE_SECONDS = 300  # 5 minutes of emptiness before auto-closing

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.queue: Deque[int] = deque()  # store user IDs
        # active: guild_id -> text_channel_id -> metadata
        self.active: Dict[int, Dict[int, Dict]] = {}
        # metadata contains: members [ids], text_channel_id, created_at, empty_since (None or datetime)
        self.cleaner_loop.start()

    async def _get_or_create_category(self, guild: discord.Guild) -> Optional[discord.CategoryChannel]:
        """Find a category named 'study partner' (case-insensitive) or create it."""
        # try to find existing category (case-insensitive)
        for c in guild.categories:
            if c.name.lower() == "study partner":
                return c
        # create it
        try:
            return await guild.create_category("study partner")
        except Exception:
            return None

    async def _cleanup_category_for_guild(self, guild: discord.Guild):
        """Delete all channels under the 'study partner' category for a guild."""
        # find category
        cat = None
        for c in guild.categories:
            if c.name.lower() == "study partner":
                cat = c
                break
        if cat is None:
            return

        # delete channels in category
        for ch in list(cat.channels):
            try:
                await ch.delete()
            except Exception:
                # ignore errors (permissions etc.)
                pass

    def _get_config_for_guild(self, guild_id: int) -> Optional[Dict]:
        # Use the SQLite-backed storage layer for guild configuration
        try:
            return storage.get_guild_config(guild_id)
        except Exception:
            return None

    def cog_unload(self):
        self.cleaner_loop.cancel()

    @commands.hybrid_command(name="findpartner", aliases=["fp", "partner", "studypartner"], description="Join the study partner queue")
    async def findpartner(self, ctx: commands.Context):
        """Join (or leave) the queue for a study partner. If someone is waiting, pair them.

        Calling the command while already queued will remove you from the queue.
        """
        author = ctx.author
        guild = ctx.guild
        if guild is None:
            await ctx.reply("This command must be used in a server/guild.")
            return

        # If this guild is configured, only allow running the command in the configured pairing channel
        conf = self._get_config_for_guild(guild.id)
        if conf is not None:
            pairing_channel_id = conf.get("pairing")
            if pairing_channel_id is not None and ctx.channel.id != pairing_channel_id:
                # inform user where they should run the command
                try:
                    pairing_mention = f"<#{pairing_channel_id}>"
                except Exception:
                    pairing_mention = str(pairing_channel_id)
                await ctx.reply(f"This command can only be used in the designated pairing channel: {pairing_mention}.")
                return

        # Prevent bot or already paired/queued users
        if author.bot:
            return

        # Check if user is already in an active pair
        for gdata in self.active.get(guild.id, {}).values():
            if author.id in gdata.get("members", []):
                await ctx.reply("You're already in an active study session.")
                return

        if author.id in self.queue:
            # Toggle: remove from queue
            try:
                self.queue.remove(author.id)
            except ValueError:
                pass
            embed = discord.Embed(
                title="Removed from the queue",
                description="You've been removed from the study partner queue.",
                color=discord.Color.dark_green(),
            )
            await ctx.reply(embed=embed)
            return

        # If someone is waiting, pair them
        if self.queue:
            other_id = self.queue.popleft()
            other = guild.get_member(other_id)
            if other is None:
                # Member may not be cached (members intent not enabled). Try fetching from API.
                try:
                    other = await guild.fetch_member(other_id)
                except Exception:
                    # The waiting user might have left the guild or cannot be fetched; skip and try again
                    return await self.findpartner(ctx)

            # Create temp channels under the 'study partner' category
            category = await self._get_or_create_category(guild)
            # fallback to the invoking channel's category if unavailable
            if category is None:
                category = ctx.channel.category
            overwrites = {
                guild.default_role: discord.PermissionOverwrite(view_channel=False),
                self.bot.user: discord.PermissionOverwrite(view_channel=True, send_messages=True, connect=True),
                author: discord.PermissionOverwrite(view_channel=True, send_messages=True, connect=True),
                other: discord.PermissionOverwrite(view_channel=True, send_messages=True, connect=True),
            }

            # Create text channel
            text_name = f"study-{author.display_name.lower()}-{other.display_name.lower()}"

            text_chan = await guild.create_text_channel(
                name=text_name[:100], overwrites=overwrites, category=category
            )

            # Prepare metadata
            meta = {
                "members": [author.id, other.id],
                "text_channel_id": text_chan.id,
                "created_at": datetime.datetime.utcnow(),
                "empty_since": None,
                "messages": [],
            }
            # store sessions keyed by text channel id
            self.active.setdefault(guild.id, {})[text_chan.id] = meta

            # Ping both users in the invoking channel using a nice embed
            pair_embed = discord.Embed(
                title="You've been paired!",
                description=(
                    f"{author.mention} and {other.mention} have been paired for a study session.\n\n"
                    f"Text channel: {text_chan.mention}\n\n"
                    "Use the text channel for coordination. Use `!close` inside the text channel when you're done."
                ),
                color=discord.Color.green(),
            )
            await ctx.send(embed=pair_embed)

            # Post a starter message in the text channel
            await text_chan.send(f"Hello {author.mention} and {other.mention}! This is your private study channel. Use `!close` here to end the session when you're done.")
            return

        # Otherwise, put user in queue
        # Otherwise, put user in queue
        self.queue.append(author.id)
        embed = discord.Embed(
            title="You've been added to the queue!",
            description=(
                "There's no one looking for a group right now, but there will be soon!\n\n"
                "Type `!findpartner` to be removed from the queue."
            ),
            color=discord.Color.green(),
        )
        # Use reply to the user message like your screenshot
        await ctx.reply(embed=embed)

    @commands.hybrid_command(name="close", description="Close your active study session (deletes temp channels and logs messages)")
    async def close(self, ctx: commands.Context):
        """Close the session associated with this channel or the invoker.

        Behavior:
        - If used inside a temp text/voice channel, closes that session.
        - Otherwise, if the invoker is a participant in a session, closes that session.
        - Logs messages to (or creates) a channel named `findpartner-logs`.
        """
        guild = ctx.guild
        if guild is None:
            await ctx.reply("This command must be used in a server/guild.")
            return

        # Determine which active session to close
        session = None
        session_voice_id = None
        # 1) If invoked in one of the temp text/voice channels
        for v_id, meta in self.active.get(guild.id, {}).items():
            if ctx.channel.id in (meta.get("text_channel_id"), meta.get("voice_channel_id")):
                session = meta
                session_voice_id = v_id
                break

        # 2) Otherwise, see if the author is a participant in any session
        if session is None:
            for v_id, meta in self.active.get(guild.id, {}).items():
                if ctx.author.id in meta.get("members", []):
                    session = meta
                    session_voice_id = v_id
                    break

        if session is None:
            await ctx.reply("No active study session found to close.")
            return

        # Allow closing if author is a participant or has manage_channels
        if ctx.author.id not in session.get("members", []) and not ctx.author.guild_permissions.manage_channels:
            await ctx.reply("You don't have permission to close this session.")
            return

        # Log messages
        text_chan = guild.get_channel(session["text_channel_id"]) if session.get("text_channel_id") else None
        transcript = []
        # Use cached messages collected via on_message
        for msg in session.get("messages", []):
            ts = msg["created_at"].isoformat()
            author_name = msg["author_name"]
            content = msg["content"]
            transcript.append(f"[{ts}] {author_name}: {content}")

        # Additionally pull recent history just-in-case
        if text_chan is not None:
            try:
                async for m in text_chan.history(limit=500):
                    # Skip if already in transcript (best effort)
                    if any(m.id == cached.get("id") for cached in session.get("messages", [])):
                        continue
                    transcript.append(f"[{m.created_at.isoformat()}] {m.author.display_name}: {m.content}")
            except Exception:
                # ignore read errors
                pass

        # Post transcript to (or create) logs channel
        # Use configured partner_log channel if present in dictionary.json
        logs_chan = None
        conf = self._get_config_for_guild(guild.id)
        if conf is not None:
            partner_log_id = conf.get("partner_log")
            if partner_log_id:
                logs_chan = guild.get_channel(partner_log_id)
        # fallback to creating a local logs channel if not configured or not found
        if logs_chan is None:
            logs_name = "findpartner-logs"
            logs_chan = discord.utils.get(guild.text_channels, name=logs_name)
            if logs_chan is None:
                try:
                    logs_chan = await guild.create_text_channel(name=logs_name)
                except Exception:
                    logs_chan = None

        if logs_chan is not None and transcript:
            # Send in chunks if large
            chunk_size = 1900
            chunk = []
            cur_len = 0
            header = f"Transcript for session between: {', '.join(str(guild.get_member(uid)) for uid in session['members'])}"
            await logs_chan.send(header)
            for line in reversed(transcript):
                if cur_len + len(line) + 1 > chunk_size:
                    await logs_chan.send("\n".join(reversed(chunk)))
                    chunk = [line]
                    cur_len = len(line)
                else:
                    chunk.append(line)
                    cur_len += len(line) + 1
            if chunk:
                await logs_chan.send("\n".join(reversed(chunk)))

        # Delete channels
        try:
            if text_chan:
                await text_chan.delete()
        except Exception:
            pass
        try:
            voice_chan = guild.get_channel(session.get("voice_channel_id"))
            if voice_chan:
                await voice_chan.delete()
        except Exception:
            pass

        # Remove session from active
        try:
            del self.active[guild.id][session_voice_id]
        except Exception:
            pass

        await ctx.reply("Session closed and logged.")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        # Capture messages in active temp text channels for transcript
        if message.author.bot:
            return
        guild = message.guild
        if guild is None:
            return
        for meta in self.active.get(guild.id, {}).values():
            if message.channel.id == meta.get("text_channel_id"):
                # cache minimal info
                meta.setdefault("messages", []).append(
                    {
                        "id": message.id,
                        "created_at": message.created_at,
                        "author_name": message.author.display_name,
                        "content": message.content,
                    }
                )

    @tasks.loop(seconds=60)
    async def cleaner_loop(self):
        """Periodic task to auto-close empty voice channels after AUTO_CLOSE_SECONDS."""
        now = datetime.datetime.utcnow()
        for guild_id, sessions in list(self.active.items()):
            guild = self.bot.get_guild(guild_id)
            if guild is None:
                continue
            for v_id, meta in list(sessions.items()):
                voice = guild.get_channel(v_id)
                if voice is None:
                    # channel deleted externally; close session
                    # simulate closure: call close logic by building a fake context? Simpler: delete metadata
                    try:
                        del self.active[guild_id][v_id]
                    except Exception:
                        pass
                    continue

                # Check occupancy
                members = [m for m in voice.members if not m.bot]
                if not members:
                    if meta.get("empty_since") is None:
                        meta["empty_since"] = now
                    else:
                        elapsed = (now - meta["empty_since"]).total_seconds()
                        if elapsed >= self.AUTO_CLOSE_SECONDS:
                            # perform closure similar to close command
                            # find the text channel and send a tiny note that it's being auto-closed
                            text_chan = guild.get_channel(meta.get("text_channel_id"))
                            if text_chan:
                                try:
                                    await text_chan.send("Session was empty for a while and will be closed automatically.")
                                except Exception:
                                    pass
                            # call the same close routine by constructing a fake context: we'll call close() helper by invoking the command's logic
                            # Instead of duplicating, call close by creating a dummy Context is cumbersome; we'll directly perform the delete + logging here
                            # reuse code: build transcript and post to logs
                            transcript = []
                            for msg in meta.get("messages", []):
                                ts = msg["created_at"].isoformat()
                                transcript.append(f"[{ts}] {msg['author_name']}: {msg['content']}")
                            # Use configured partner_log if available
                            logs_chan = None
                            conf = self._get_config_for_guild(guild.id)
                            if conf is not None:
                                partner_log_id = conf.get("partner_log")
                                if partner_log_id:
                                    logs_chan = guild.get_channel(partner_log_id)
                            if logs_chan is None:
                                try:
                                    logs_chan = discord.utils.get(guild.text_channels, name="findpartner-logs")
                                    if logs_chan is None:
                                        logs_chan = await guild.create_text_channel(name="findpartner-logs")
                                except Exception:
                                    logs_chan = None
                            if logs_chan is not None and transcript:
                                await logs_chan.send(f"Auto-closed transcript for session between: {', '.join(str(guild.get_member(uid)) for uid in meta['members'])}")
                                # send as a single message if small
                                await logs_chan.send("\n".join(reversed(transcript)))

                            # delete channels
                            try:
                                if text_chan:
                                    await text_chan.delete()
                            except Exception:
                                pass
                            try:
                                if voice:
                                    await voice.delete()
                            except Exception:
                                pass
                            # remove from active
                            try:
                                del self.active[guild_id][v_id]
                            except Exception:
                                pass
                else:
                    # reset empty timer
                    meta["empty_since"] = None

    @cleaner_loop.before_loop
    async def before_cleaner(self):
        await self.bot.wait_until_ready()


async def setup(bot: commands.Bot):
    cog = StudyPartner(bot)
    await bot.add_cog(cog)
    # cleanup any leftover channels in the 'study partner' category across guilds
    for g in bot.guilds:
        try:
            await cog._cleanup_category_for_guild(g)
        except Exception:
            pass