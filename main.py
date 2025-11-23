import discord, dotenv, os, glob
from typing import List
from discord import app_commands
from discord.ext import commands
import storage

dotenv.load_dotenv()


class AdminCog(commands.Cog):
    """Admin commands: owner utilities plus per-guild cog enable/disable for server admins."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def autocomplete_cog(self, interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
        base_dir = os.path.dirname(__file__)
        cogs_dir = os.path.join(base_dir, "cogs")
        py_files = glob.glob(os.path.join(cogs_dir, "*.py"))
        names = [os.path.splitext(os.path.basename(p))[0] for p in py_files if os.path.splitext(os.path.basename(p))[0] != "__init__"]
        matches = [n for n in names if current.lower() in n.lower()]
        return [app_commands.Choice(name=n, value=n) for n in matches[:25]]

    @app_commands.autocomplete(cog=lambda interaction, current: AdminCog.autocomplete_cog(None, interaction, current))
    @commands.hybrid_command(name="reload", description="Reload cogs (optionally provide a cog name from the cogs/ folder)")
    @commands.is_owner()
    async def reload(self, ctx: commands.Context, cog: str = None):
        deferred = False
        if getattr(ctx, "interaction", None) is not None:
            try:
                await ctx.defer()
                deferred = True
            except Exception:
                try:
                    await ctx.interaction.response.defer()
                    deferred = True
                except Exception:
                    deferred = False

        base_dir = os.path.dirname(__file__)
        cogs_dir = os.path.join(base_dir, "cogs")
        py_files = glob.glob(os.path.join(cogs_dir, "*.py"))
        available = [os.path.splitext(os.path.basename(p))[0] for p in py_files if os.path.splitext(os.path.basename(p))[0] != "__init__"]

        targets = []
        if cog:
            cog_name = cog.replace('.py','')
            if cog_name in available:
                targets = [f"cogs.{cog_name}"]
            elif cog_name.startswith('cogs.') and cog_name.split('.',1)[1] in available:
                targets = [cog_name]
            else:
                await ctx.reply(f"Cog '{cog}' not found in cogs/ folder.")
                return
        else:
            targets = [f"cogs.{n}" for n in available]

        succeeded = []
        failed = []
        for ext in targets:
            try:
                if ext in self.bot.extensions:
                    await self.bot.reload_extension(ext)
                else:
                    await self.bot.load_extension(ext)
                succeeded.append(ext)
            except Exception as e:
                failed.append(f"{ext}: {e}")

        embed = discord.Embed(title="Reload Cogs Report", color=discord.Color.blue())
        embed.add_field(name="Succeeded", value="\n".join(succeeded) if succeeded else "None", inline=False)
        if failed:
            embed.add_field(name="Failed", value="\n".join(failed[:10]), inline=False)
            if len(failed) > 10:
                embed.set_footer(text=f"And {len(failed)-10} more failures...")

        try:
            synced = await self.bot.tree.sync()
            sync_result = f"Synced {len(synced)} app commands."
        except Exception as e:
            sync_result = f"Sync failed: {e}"

        embed.add_field(name="Command Sync", value=sync_result, inline=False)
        if deferred and getattr(ctx, "interaction", None) is not None:
            try:
                await ctx.interaction.followup.send(embed=embed)
                return
            except Exception:
                pass

        await ctx.reply(embed=embed)

    @commands.hybrid_command(name="sync", description="Sync application (slash) commands with Discord")
    @commands.is_owner()
    async def sync(self, ctx: commands.Context):
        deferred = False
        if getattr(ctx, "interaction", None) is not None:
            try:
                await ctx.defer()
                deferred = True
            except Exception:
                try:
                    await ctx.interaction.response.defer()
                    deferred = True
                except Exception:
                    deferred = False

        try:
            synced = await self.bot.tree.sync()
            msg = f"Synced {len(synced)} app commands."
        except Exception as e:
            msg = f"Sync failed: {e}"

        if deferred and getattr(ctx, "interaction", None) is not None:
            try:
                await ctx.interaction.followup.send(msg)
                return
            except Exception:
                pass

        await ctx.reply(msg)

    @app_commands.autocomplete(cog=lambda interaction, current: AdminCog.autocomplete_cog(None, interaction, current))
    @commands.hybrid_command(name="cog_enable", description="Enable a cog for this guild (manage_guild required)")
    @commands.has_guild_permissions(manage_guild=True)
    async def enable_cog(self, ctx: commands.Context, cog: str):
        if ctx.guild is None:
            await ctx.reply("This command must be used in a server/guild.")
            return
        cog_name = cog.replace('.py','')
        module = f"cogs.{cog_name}" if not cog_name.startswith('cogs.') else cog_name
        try:
            storage.set_cog_enabled(ctx.guild.id, module, True)
            await ctx.reply(f"Enabled {module} for this guild.")
        except Exception as e:
            await ctx.reply(f"Failed to enable cog: {e}")

    @app_commands.autocomplete(cog=lambda interaction, current: AdminCog.autocomplete_cog(None, interaction, current))
    @commands.hybrid_command(name="cog_disable", description="Disable a cog for this guild (manage_guild required)")
    @commands.has_guild_permissions(manage_guild=True)
    async def disable_cog(self, ctx: commands.Context, cog: str):
        if ctx.guild is None:
            await ctx.reply("This command must be used in a server/guild.")
            return
        cog_name = cog.replace('.py','')
        module = f"cogs.{cog_name}" if not cog_name.startswith('cogs.') else cog_name
        try:
            storage.set_cog_enabled(ctx.guild.id, module, False)
            await ctx.reply(f"Disabled {module} for this guild.")
        except Exception as e:
            await ctx.reply(f"Failed to disable cog: {e}")
        # attempt to sync application commands after changing cog availability
        try:
            synced = await self.bot.tree.sync()
            await ctx.reply(f"Synced {len(synced)} app commands.")
        except Exception as e:
            await ctx.reply(f"Sync failed: {e}")


class MyBot(commands.Bot):
    async def setup_hook(self):
        # load existing cogs and register AdminCog
        await self.load_extension("cogs.study_partner")
        await self.add_cog(AdminCog(self))


intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = MyBot(command_prefix="!", intents=intents)
bot.run(os.getenv("token"))