import time
import math
import os
from typing import List, Dict, Optional

import discord
from discord import app_commands
from discord.ext import commands

import storage

# Use TEST roles/channels when PING_USE_TEST=1|true in environment
PING_USE_TEST = str(os.getenv("PING_USE_TEST", "0")).lower() in ("1", "true", "yes")


def _get_subjects_for_guild(guild_id: Optional[int]) -> List[Dict]:
    """Return the list of subject dicts for a guild id by querying storage."""
    return storage.get_subjects_for_guild(guild_id)


def _build_ping_choices() -> List[app_commands.Choice[str]]:
    choices: List[app_commands.Choice[str]] = []
    try:
        rows = storage.get_all_pings()
        for r in rows:
            choices.append(app_commands.Choice(name=r.get("name", ""), value=r.get("value", "")))
    except Exception:
        pass
    return choices


def _build_subject_choices() -> List[app_commands.Choice[str]]:
    choices: List[app_commands.Choice[str]] = []
    try:
        rows = storage.get_all_subjects()
        for s in rows:
            choices.append(app_commands.Choice(name=s, value=s))
    except Exception:
        pass
    return choices


async def ping_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    """Provide dynamic autocomplete choices for /ping based on the invoking guild's configured pings."""
    guild_id = interaction.guild.id if interaction.guild else None
    subs = _get_subjects_for_guild(guild_id)
    choices: List[app_commands.Choice[str]] = []
    cur = (current or "").lower()
    for item in subs:
        names = item.get("names", [])
        pings = item.get("pings", [])
        for i, name in enumerate(names):
            if i >= len(pings):
                continue
            value = pings[i]
            if not value:
                continue
            if not cur or cur in name.lower() or cur in value.lower():
                choices.append(app_commands.Choice(name=name, value=value))
            if len(choices) >= 25:
                return choices
    return choices


async def subject_autocomplete(interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
    """Autocomplete subjects for commands like /addping based on subjects available for the guild."""
    guild_id = interaction.guild.id if interaction.guild else None
    subs = _get_subjects_for_guild(guild_id)
    choices: List[app_commands.Choice[str]] = []
    cur = (current or "").lower()
    for item in subs:
        name = item.get("subject", "")
        if not name:
            continue
        if not cur or cur in name.lower():
            choices.append(app_commands.Choice(name=name, value=name))
        if len(choices) >= 25:
            break
    return choices


def embed_for_role(role_name: str) -> discord.Embed:
    embed = discord.Embed(title=f"{role_name} Helper", color=0xFFC09C)
    embed.add_field(name=" ", value="Please be patient, a helper will be with you shortly.", inline=True)
    embed.add_field(name=" ", value="If you have further questions, please contact staff.\n\nGohar's Guide Staff Team", inline=False)
    return embed


def remaining_text(last_time: float, cooldown: int = 600) -> str:
    """Return a Discord relative timestamp tag for when the cooldown ends.

    Example: "<t:1700000000:R>". The timestamp is the UTC epoch (seconds) when the
    cooldown will finish (last_time + cooldown).
    """
    try:
        end_ts = int(last_time + int(cooldown))
    except Exception:
        end_ts = int(time.time())
    return f"<t:{end_ts}:R>"


class PingHelper(commands.Cog):
    """Ping helper system.

    Commands:
     - /ping <subject> (hybrid) or !ping <subject>
     - prefix shortcut: !<ping_value>
     - admin: addsubject, addping (manage_guild required)
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # No global subjects loaded into cog; we'll read per-guild when needed
        self.subjects: List[Dict] = []
        # helper maps for quick lookup (per-guild)
        self.ping_map: Dict[str, Dict] = {}

    def _rebuild_maps(self):
        self.ping_map = {}
        for item in self.subjects:
            pings = item.get("pings", [])
            # map canonical ping values (case-insensitive)
            for i, ping in enumerate(pings):
                if not ping:
                    continue
                # store both raw lower and a normalized alphanumeric-only key
                key = ping.lower()
                self.ping_map[key] = {"subject_item": item, "index": i}
                # normalized (strip non-alphanumeric) form
                norm_key = ''.join(ch for ch in key if ch.isalnum())
                if norm_key and norm_key not in self.ping_map:
                    self.ping_map[norm_key] = {"subject_item": item, "index": i}

            # map aliases; support several shapes:
            # - list of lists: aliases per-ping (aliases[i] -> list of aliases for pings[i])
            # - flat list with same length as pings: aliases[i] -> alias string for pings[i]
            # - flat list for a single-ping subject: aliases apply to pings[0]
            aliases = item.get("aliases", [])
            if isinstance(aliases, list) and aliases:
                # detect list-of-lists
                if all(isinstance(x, list) for x in aliases):
                    for i, alias_list in enumerate(aliases):
                        if not isinstance(alias_list, list):
                            continue
                        for a in alias_list:
                            if a:
                                k = str(a).lower()
                                self.ping_map[k] = {"subject_item": item, "index": i}
                                nk = ''.join(ch for ch in k if ch.isalnum())
                                if nk and nk not in self.ping_map:
                                    self.ping_map[nk] = {"subject_item": item, "index": i}
                elif len(aliases) == len(pings) and all(isinstance(x, str) for x in aliases):
                    # per-ping single alias strings
                    for i, a in enumerate(aliases):
                        if a:
                            k = str(a).lower()
                            self.ping_map[k] = {"subject_item": item, "index": i}
                            nk = ''.join(ch for ch in k if ch.isalnum())
                            if nk and nk not in self.ping_map:
                                self.ping_map[nk] = {"subject_item": item, "index": i}
                else:
                    # flat list: apply all aliases to the first ping (common case when subject has single ping)
                    target_idx = 0 if len(pings) > 0 else None
                    if target_idx is not None:
                        for a in aliases:
                            if a:
                                k = str(a).lower()
                                self.ping_map[k] = {"subject_item": item, "index": target_idx}
                                nk = ''.join(ch for ch in k if ch.isalnum())
                                if nk and nk not in self.ping_map:
                                    self.ping_map[nk] = {"subject_item": item, "index": target_idx}

    def _load_for_guild(self, guild_id: Optional[int]):
        """Load subjects for a guild into self.subjects and rebuild the ping_map."""
        self.subjects = _get_subjects_for_guild(guild_id)
        self._rebuild_maps()

    @commands.hybrid_command(name="ping", description="Ping a homework helper")
    @app_commands.autocomplete(subject=ping_autocomplete)
    async def ping(self, ctx: commands.Context, subject: Optional[str] = None):
        """Ping a helper by ping id (value from choices).

        If called without a subject, respond with bot latency and guidance to list pings.
        """
        # If no subject provided, return latency and a hint
        if subject is None:
            latency_ms = round(self.bot.latency * 1000)
            emb = discord.Embed(title="Pong!", description=f"{latency_ms}ms\n\nRun `/listpings` to see available pings.", color=discord.Color.green())
            # if interaction, reply with embed; otherwise plain reply
            try:
                if getattr(ctx, "interaction", None):
                    await ctx.reply(embed=emb)
                else:
                    await ctx.reply(f"Pong! {latency_ms}ms — run /listpings to see available pings.")
            except Exception:
                # best-effort fallback
                await ctx.send(f"Pong! {latency_ms}ms")
            return

        # ensure we have the map for this guild
        guild_id = ctx.guild.id if ctx.guild else None
        self._load_for_guild(guild_id)

        # find ping (case-insensitive, support aliases). Normalize subject to ignore non-alphanumerics.
        if not isinstance(subject, str):
            lookup = None
        else:
            norm = ''.join(ch for ch in subject.lower() if ch.isalnum())
            lookup = self.ping_map.get(subject) or self.ping_map.get(subject.lower()) or (self.ping_map.get(norm) if norm else None)
        if not lookup:
            await ctx.reply("Unknown subject.")
            return

        item = lookup["subject_item"]
        idx = lookup["index"]
        # optional channel restriction: prefer test_channel if configured in env
        channel_limit = item.get("test_channel") if PING_USE_TEST else item.get("channel")
        if channel_limit and ctx.channel.id != channel_limit:
            await ctx.reply(f"This ping can only be used in <#{channel_limit}>.", ephemeral=True if getattr(ctx, 'interaction', None) else False)
            return

        times = item.setdefault("times", [0]*len(item.get("pings", [])))
        last = times[idx] if idx < len(times) else 0
        if time.time() - last >= 600:
            # choose role from main or test lists
            roles_main = item.get("roles", [])
            roles_test = item.get("roles_test", [])
            role_id = None
            if PING_USE_TEST and idx < len(roles_test) and roles_test[idx]:
                role_id = roles_test[idx]
            elif idx < len(roles_main):
                role_id = roles_main[idx]
            role_mention = f"<@&{role_id}>" if role_id else ""
            # send embed and ping
            try:
                await ctx.send(f"{role_mention}", embed=embed_for_role(item.get("names", [])[idx]))
            except Exception:
                # fallback to reply
                await ctx.reply(f"{role_mention}")
            # update time in-memory
            if idx < len(times):
                times[idx] = time.time()
            else:
                while len(times) <= idx:
                    times.append(0)
                times[idx] = time.time()
            # persist into SQLite
            try:
                ping_value = item.get("pings", [])[idx]
                storage.update_ping_time(guild_id or 0, item.get("subject"), ping_value, time.time())
            except Exception:
                pass
        else:
            # cooldown
            rem = remaining_text(last)
            # if interaction make ephemeral reply
            if getattr(ctx, "interaction", None):
                await ctx.reply(f"This ping is on cooldown\nIt can be used again {rem}", ephemeral=True)
            else:
                await ctx.reply(f"This ping is on cooldown\nIt can be used again {rem}")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        # allow shortcut !<ping_value>
        if message.author.bot:
            return
        content = message.content.strip()
        if not content.startswith("!"):
            return
        body = content[1:]
        # ignore typical commands like !ping handled above
        if body.startswith("ping "):
            return
        # split args: only take first token
        token = body.split()[0]
        # normalize token: ignore non-alphanumeric characters for prefix lookups
        token_norm = ''.join(ch for ch in token.lower() if ch.isalnum())
        # load mapping for this guild
        self._load_for_guild(message.guild.id if message.guild else None)
        lookup = self.ping_map.get(token) or self.ping_map.get(token.lower()) or (self.ping_map.get(token_norm) if token_norm else None)
        if not lookup:
            return
        # create a fake Context-like object: use message.channel and author
        ctx = await self.bot.get_context(message)
        await self.ping(ctx, token)

    @commands.hybrid_command(name="addsubject", description="Add a new subject category")
    @commands.has_guild_permissions(manage_guild=True)
    async def addsubject(self, ctx: commands.Context, subject: str):
        if ctx.guild is None:
            await ctx.reply("This command must be used in a server/guild.")
            return
        try:
            storage.create_subject(ctx.guild.id, subject)
            self._load_for_guild(ctx.guild.id)
            await ctx.reply("Subject added.")
        except Exception:
            await ctx.reply("Failed to add subject.")

    @app_commands.autocomplete(subject=subject_autocomplete)
    @commands.hybrid_command(name="addping", aliases=["createping", "newping"], description="Create a new helper ping option to a subject")
    @commands.has_guild_permissions(manage_guild=True)
    async def addping(self, ctx: commands.Context, subject: str, ping: str, name: str, role: discord.Role):
        if ctx.guild is None:
            await ctx.reply("This command must be used in a server/guild.")
            return
        ok = storage.add_ping(ctx.guild.id, subject, ping, name, role)
        if ok:
            self._load_for_guild(ctx.guild.id)
            await ctx.reply("Ping added.")
        else:
            await ctx.reply("Subject not found or failed to add ping.")

    @commands.hybrid_command(name="listpings", aliases=["listping", "lp"], description="List available pings or show details for one")
    async def listpings(self, ctx: commands.Context, command: Optional[str] = None):
        """Show all pings organized by subject, or details for a specific ping (use the ping value).

        Examples:
        - /listpings
        - /listpings <command>
        """
        # reload subjects from storage for this guild in case they were edited externally
        self._load_for_guild(ctx.guild.id if ctx.guild else None)

        if command:
            lookup = self.ping_map.get(command)
            if not lookup:
                await ctx.reply("Could not find that ping.")
                return
            item = lookup["subject_item"]
            idx = lookup["index"]
            name = item.get("names", [])[idx] if idx < len(item.get("names", [])) else ""
            role_id = item.get("roles", [])[idx] if idx < len(item.get("roles", [])) else None
            # Last-used info (for remaining cooldown display)
            last = 0
            times = item.get("times", [])
            if idx < len(times):
                last = times[idx]

            # Determine configured cooldown (in seconds). Default to 600 (10m).
            cfg_cd = item.get("cooldown", 600)
            # Format cooldown for display (e.g., 10m, 30s)
            def _fmt_cd(sec: int) -> str:
                if sec >= 60 and sec % 60 == 0:
                    return f"{sec//60}m"
                if sec >= 60:
                    return f"{math.ceil(sec/60)}m"
                return f"{sec}s"

            rem = remaining_text(last, cooldown=cfg_cd)

            embed = discord.Embed(title=f"Role Ping", color=discord.Color.blue())
            embed.add_field(name="command:", value=command, inline=False)
            embed.add_field(name="role:", value=(f"<@&{role_id}>" if role_id else "(none)"), inline=False)
            # message and footer optional fields
            msg = item.get("message")
            embed.add_field(name="message:", value=(msg if msg else "None set (uses default)"), inline=False)
            footer = item.get("footer")
            embed.add_field(name="footer:", value=(footer if footer else "None set (uses default)"), inline=False)
            embed.add_field(name="cooldown:", value=_fmt_cd(cfg_cd), inline=False)
            ch = item.get("channel", 0)
            embed.add_field(name="channels:", value=(f"<#{ch}>" if ch else "(none)"), inline=False)
            aliases = item.get("aliases", []) or []
            if isinstance(aliases, list):
                alias_text = ", ".join([f"`{a}`" for a in aliases]) if aliases else "(none)"
            else:
                alias_text = f"`{aliases}`"
            embed.add_field(name="aliases:", value=alias_text, inline=False)
            # category maps to subject or explicit category field
            cat = item.get("category") or item.get("subject") or "(none)"
            embed.add_field(name="category:", value=cat, inline=False)
            await ctx.reply(embed=embed)
            return

        # Build full listing
        embed = discord.Embed(title="Role Pings", color=discord.Color.green())
        for item in self.subjects:
            subject_name = item.get("subject", "Misc")
            lines: List[str] = []
            for ping in item.get("pings", []):
                lines.append(f"`!{ping}`")
            value = "\n".join(lines) if lines else "(no pings)"
            # If the field text is long, truncate safely
            if len(value) > 1000:
                value = value[:980] + "\n..."
            embed.add_field(name=subject_name, value=value, inline=False)

        embed.set_footer(text="Use !listpings <command> to see more info on a role ping.")
        await ctx.reply(embed=embed)


async def setup(bot: commands.Bot):
    await bot.add_cog(PingHelper(bot))
