import asyncio
import io
import os
from typing import Tuple

import discord
from discord.ext import commands


class GitControl(commands.Cog):
    """Owner-only commands to run git commands on the bot's repo."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def _run_cmd(self, cmd: str) -> Tuple[str, int]:
        """Run a shell command in the repository root and return (output, returncode)."""
        cwd = os.getcwd()
        proc = await asyncio.create_subprocess_shell(
            cmd,
            cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        out, _ = await proc.communicate()
        try:
            text = out.decode("utf-8", errors="replace")
        except Exception:
            text = str(out)
        return text, proc.returncode

    @commands.command(name="gitstatus", hidden=True)
    @commands.is_owner()
    async def gitstatus(self, ctx: commands.Context):
        """Show `git status -sb` for the repository."""
        try:
            await ctx.channel.trigger_typing()
        except Exception:
            # fallback to context manager if available
            try:
                async with ctx.typing():
                    pass
            except Exception:
                pass
        out, rc = await self._run_cmd("git status -sb")
        header = f"`git status -sb` exited with {rc}\n"
        if not out:
            await ctx.reply(header + "(no output)")
            return
        if len(header) + len(out) < 1900:
            await ctx.reply(header + "```\n" + out + "```")
            return
        bio = io.BytesIO(out.encode("utf-8"))
        bio.seek(0)
        await ctx.reply(header + "output too long, sending as file", file=discord.File(bio, filename="git-status.log"))

    @commands.command(name="gitpull", aliases=["pull", "update"], hidden=True)
    @commands.is_owner()
    async def gitpull(self, ctx: commands.Context):
        """Run `git pull` in the repo root and return the output. Owner-only."""
        await ctx.reply("Running `git pull`...")
        out, rc = await self._run_cmd("git pull")
        header = f"`git pull` exited with {rc}\n"
        if not out:
            await ctx.send(header + "(no output)")
            return
        if len(header) + len(out) < 1900:
            await ctx.send(header + "```\n" + out + "```")
            return
        bio = io.BytesIO(out.encode("utf-8"))
        bio.seek(0)
        await ctx.send(header + "output too long, sending as file", file=discord.File(bio, filename="git-pull.log"))


async def setup(bot: commands.Bot):
    await bot.add_cog(GitControl(bot))
