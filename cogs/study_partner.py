import asyncio
import datetime
import io
import json
import os
from collections import deque
from typing import Deque, Dict, List, Optional
import storage

import discord
from discord.ext import commands, tasks

# persistence moved to SQLite (storage.py)


class StudyPartner(commands.Cog):
    """Find study partners by queueing users, creating temporary text channels,
    reacting to the invoker when paired, and logging the text channel messages on close.

    Features:
    - `findpartner` (no args): join the queue; if someone waiting, pair them.
    - creates a private text channel for the pair and reacts to the invoker when paired
    - `close`: closes the pair channel, posts a transcript to `#findpartner-logs` and deletes the channel
    - automatic closing when the text channel is inactive for a configurable timeout
    """
    
    AUTO_CLOSE_SECONDS = 300  # 5 minutes of emptiness before auto-closing

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.queue: Deque[int] = deque()  # store user IDs
        # active: text_channel_id -> metadata (global across all servers)
        self.active: Dict[int, Dict] = {}
        # metadata contains: members [ids], text_channel_id, created_at, empty_since (None or datetime)
        self.cleaner_loop.start()
        # Load persisted queue and sessions from storage
        try:
            q = storage.load_queue()
            if q:
                self.queue = deque(q)
        except Exception:
            pass
        try:
            sessions = storage.load_sessions()
            for s in sessions:
                tid = s.get("text_channel_id")
                if tid is None:
                    continue
                # normalize created_at to datetime (aware)
                ca = s.get("created_at")
                if isinstance(ca, str):
                    try:
                        ca_dt = datetime.datetime.fromisoformat(ca)
                    except Exception:
                        ca_dt = datetime.datetime.now(datetime.timezone.utc)
                elif isinstance(ca, datetime.datetime):
                    ca_dt = ca
                else:
                    ca_dt = datetime.datetime.now(datetime.timezone.utc)
                # normalize messages' created_at to datetime
                msgs = []
                for m in s.get("messages", []):
                    mcopy = dict(m)
                    ca_m = mcopy.get("created_at")
                    if isinstance(ca_m, str):
                        try:
                            mcopy["created_at"] = datetime.datetime.fromisoformat(ca_m)
                        except Exception:
                            mcopy["created_at"] = None
                    msgs.append(mcopy)
                meta = {
                    "members": s.get("members", []),
                    "text_channel_id": int(tid),
                    "created_at": ca_dt,
                    "empty_since": None,
                    "messages": msgs,
                }
                self.active[int(tid)] = meta
        except Exception:
            pass

    async def _get_or_create_category(self, guild: discord.Guild) -> Optional[discord.CategoryChannel]:
        """Find the category or create it."""
        # try to find existing category (case-insensitive)
        conf = self._get_config()
        channel_id = conf.get("pairing") if conf else None
        channel = guild.get_channel(channel_id) if channel_id else None
        category = channel.category if channel else None
        if category is not None:
            return category
        # create it
        try:
            return await guild.create_category("Matchmaking")
        except Exception:
            return None

    async def _cleanup_category_for_guild(self, guild: discord.Guild):
        """Delete all channels under the find partner category for a guild."""
        # find category
        try:
            conf = self._get_config()
            channel_id = conf.get("pairing") if conf else None
            channel = guild.get_channel(channel_id) if channel_id else None
            category = channel.category if channel else None
        except Exception:
            category = None


        # delete channels in category
        for ch in list(category.channels):
            try:
                if ch.id != channel_id:
                    await ch.delete()
            except Exception:
                # ignore errors (permissions etc.)
                pass

    def _get_config(self) -> Optional[Dict]:
        # Read global configuration values from storage (no guild context)
        try:
            return storage.get_guild_config()
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
        conf = self._get_config()
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

        # Check if user is already in an active pair (global)
        for gdata in self.active.values():
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
            try:
                storage.save_queue(list(self.queue))
            except Exception:
                pass
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
                self.bot.user: discord.PermissionOverwrite(view_channel=True, send_messages=True),
                author: discord.PermissionOverwrite(view_channel=True, send_messages=True),
                other: discord.PermissionOverwrite(view_channel=True, send_messages=True),
            }

            # Create text channel
            text_name = f"study-{author.name.lower()}-{other.name.lower()}"

            text_chan = await guild.create_text_channel(
                name=text_name[:100], overwrites=overwrites, category=category
            )

            # Prepare metadata
            meta = {
                "members": [author.id, other.id],
                "text_channel_id": text_chan.id,
                "created_at": datetime.datetime.now(datetime.timezone.utc),
                "empty_since": None,
                "messages": [],
            }
            # store sessions keyed by text channel id (global)
            self.active[text_chan.id] = meta

            try:
                storage.save_session(meta)
            except Exception:
                pass

            # React to the invoking message to indicate pairing
            try:
                await ctx.message.add_reaction("✅")
            except Exception:
                # ignore if reaction fails (permissions etc.)
                pass

            # Post a starter message in the text channel
            await text_chan.send(f"Hello {author.mention} and {other.mention}! This is your private study channel. Use `!close` here to end the session when you're done.")
            return

        # Otherwise, put user in queue
        # Otherwise, put user in queue
        self.queue.append(author.id)
        try:
            storage.save_queue(list(self.queue))
        except Exception:
            pass
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
        - If used inside a temp text channel, closes that session.
        - Otherwise, if the invoker is a participant in a session, closes that session.
        - Logs messages to (or creates) a channel named `findpartner-logs`.
        """
        guild = ctx.guild
        if guild is None:
            await ctx.reply("This command must be used in a server/guild.")
            return

        # Determine which active session to close (search global sessions)
        session = None
        session_id = None
        # 1) If invoked in the session text channel
        for tid, meta in self.active.items():
            if ctx.channel.id == meta.get("text_channel_id"):
                session = meta
                session_id = tid
                break

        # 2) Otherwise, see if the author is a participant in any session
        if session is None:
            for tid, meta in self.active.items():
                if ctx.author.id in meta.get("members", []):
                    session = meta
                    session_id = tid
                    break

        if session is None:
            await ctx.reply("No active study session found to close.")
            return

        # Allow closing if author is a participant or has manage_channels
        if ctx.author.id not in session.get("members", []) and not ctx.author.guild_permissions.manage_channels:
            await ctx.reply("You don't have permission to close this session.")
            return

        # Delegate transcript building and cleanup to helper

        # Log and cleanup via helper (which will also attempt to delete persisted session and channel)
        closure_reason = "Closed by command"
        try:
            await self._log_session(guild, session, closure_reason)
        except Exception:
            pass

        # Ensure session removed from active map if still present
        try:
            if session_id in self.active:
                del self.active[session_id]
        except Exception:
            pass

        try:
            await ctx.reply("Session closed and logged.")
        except Exception:
            pass

    async def _log_session(self, guild: discord.Guild, session: Dict, closure_reason: str) -> None:
        """Helper to post a transcript as a .log file to the configured logs channel.

        This builds the transcript from cached messages and channel history,
        posts it to the configured logs channel, then attempts to delete the
        persisted session, remove it from `self.active`, and delete the text channel.
        """
        if guild is None or session is None:
            return

        text_chan = None
        if session.get("text_channel_id"):
            try:
                text_chan = guild.get_channel(session.get("text_channel_id"))
            except Exception:
                text_chan = None

        # Always build transcript from cached messages and channel history
        transcript: List[str] = []
        for msg in session.get("messages", []):
            ca = msg.get("created_at")
            if isinstance(ca, datetime.datetime):
                ts = ca.isoformat()
            else:
                ts = str(ca)
            transcript.append(f"[{ts}] {msg.get('author_name')}: {msg.get('content')}")

        if text_chan is not None:
            try:
                cached_ids = set(c.get("id") for c in session.get("messages", []))
            except Exception:
                cached_ids = set()
            try:
                async for m in text_chan.history(limit=500, oldest_first=True):
                    if m.id in cached_ids:
                        continue
                    transcript.append(f"[{m.created_at.isoformat()}] {m.author.name} ({m.author.id}): {m.content}")
            except Exception:
                pass

        # Find or create logs channel (respect configured partner_log)
        logs_chan = None
        try:
            conf = self._get_config()
        except Exception:
            conf = None
        if conf is not None:
            partner_log_id = conf.get("partner_log")
            if partner_log_id:
                try:
                    logs_chan = guild.get_channel(partner_log_id)
                except Exception:
                    logs_chan = None

        if logs_chan is None:
            try:
                logs_chan = discord.utils.get(guild.text_channels, name="findpartner-logs")
                if logs_chan is None:
                    logs_chan = await guild.create_text_channel(name="findpartner-logs")
            except Exception:
                logs_chan = None

        if logs_chan is None or not transcript:
            return

        header = f"Transcript for session between: {', '.join(str(uid) for uid in session.get('members', []))} in channel {session.get('text_channel_id', 'unknown')}"

        heading = (
            f"----- PARTNER CHANNEL LOG -----\n\n"
            f"Channel ID: {session.get('text_channel_id', 'unknown')}\n"
            f"Created At: {session.get('created_at').isoformat() if session.get('created_at') else 'unknown'}\n"
            f"Closed At: {datetime.datetime.utcnow().isoformat()}\n"
            f"Closure Reason: {closure_reason}\n"
            f"Members: {', '.join(str(guild.get_member(uid)) for uid in session.get('members', []))}\n"
            f"Channel Name: {getattr(guild.get_channel(session.get('text_channel_id')), 'name', 'unknown')}\n\n"
        )

        content = heading + "\n".join(transcript)
        bio = io.BytesIO(content.encode("utf-8"))
        bio.seek(0)
        created = session.get("created_at")
        if isinstance(created, datetime.datetime):
            ts = created.strftime("%Y%m%dT%H%M%SZ")
        else:
            ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        fname = f"session-{session.get('text_channel_id', 'unknown')}-{ts}.log"

        try:
            await logs_chan.send(header, file=discord.File(bio, filename=fname))
        except Exception:
            # fallback to chunked messages
            try:
                await logs_chan.send(header)
                for chunk in (content[i : i + 1900] for i in range(0, len(content), 1900)):
                    await logs_chan.send(chunk)
            except Exception:
                pass

        # Cleanup: delete persisted session, remove from active, delete channel
        try:
            sid = session.get("text_channel_id")
            if sid is not None:
                try:
                    storage.delete_session(sid)
                except Exception:
                    pass
                try:
                    if sid in self.active:
                        del self.active[sid]
                except Exception:
                    pass
        except Exception:
            pass

        try:
            if text_chan is not None:
                await text_chan.delete()
        except Exception:
            pass


    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        # Capture messages in active temp text channels for transcript
        if message.author.bot:
            return
        # Capture messages in active temp text channels for transcript
        if message.guild is None:
            return
        for meta in self.active.values():
            if message.channel.id == meta.get("text_channel_id"):
                # cache minimal info
                meta.setdefault("messages", []).append({
                    "id": message.id,
                    "created_at": message.created_at,
                    "author_name": message.author.display_name,
                    "content": message.content,
                })
                try:
                    storage.save_session(meta)
                except Exception:
                    pass

    @tasks.loop(seconds=60)
    async def cleaner_loop(self):
        """Periodic task to auto-close inactive text channels after AUTO_CLOSE_SECONDS.

        Determines inactivity by checking the latest cached message timestamp (or falling
        back to channel history). When inactive beyond the threshold, posts a .log file
        to the logs channel and deletes the text channel.
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        # active is global mapping: text_channel_id -> meta
        for text_id, meta in list(self.active.items()):
            text_chan = self.bot.get_channel(text_id)
            if text_chan is None:
                try:
                    del self.active[text_id]
                except Exception:
                    pass
                continue

            # Determine last activity timestamp
            last_ts = None
            if meta.get("messages"):
                last_ts = meta.get("messages")[-1].get("created_at")
            else:
                try:
                    async for m in text_chan.history(limit=1):
                        last_ts = m.created_at
                        break
                except Exception:
                    last_ts = None

            if last_ts is None:
                last_ts = meta.get("created_at")

            # normalize last_ts when it's a string
            if isinstance(last_ts, str):
                try:
                    last_ts = datetime.datetime.fromisoformat(last_ts)
                except Exception:
                    last_ts = None

            if last_ts is None:
                # nothing to base inactivity on; skip
                continue

            elapsed = (now - last_ts).total_seconds()
            if elapsed < self.AUTO_CLOSE_SECONDS:
                # still active
                continue

            # perform auto-close
            try:
                await text_chan.send("Session was inactive and will be closed automatically.")
            except Exception:
                pass

            # Use helper to post transcript and perform cleanup
            try:
                guild = getattr(text_chan, "guild", None)
                await self._log_session(guild, meta, "Auto-closed due to inactivity")
            except Exception:
                pass
            try:
                if text_id in self.active:
                    del self.active[text_id]
            except Exception:
                pass

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