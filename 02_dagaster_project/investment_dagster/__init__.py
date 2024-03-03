# fmt: off
from dagster import Definitions, load_assets_from_modules

from .assets import bot

bot_assets = load_assets_from_modules([bot])

defs = Definitions(
    assets=[*bot_assets]
)
