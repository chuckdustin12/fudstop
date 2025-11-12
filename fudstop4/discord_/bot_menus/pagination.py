import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))


from typing import List
import disnake

class AlertMenus(disnake.ui.View):
    def __init__(self, embeds: List[disnake.Embed] = None):
        
        super().__init__(timeout=None)

        self.embeds = embeds if embeds is not None else []
        self.embed_count = 0
        self.prev_page.disabled = True
        self.count = 0




    @disnake.ui.button(
        style=disnake.ButtonStyle.red,
        custom_id=f"persistent_view:prevqwfpage_{str(disnake.Member)}aq2wfwa",
        row=4,
        label=f"🇵 🇷 🇪 🇻"

    )
    async def prev_page(  # pylint: disable=W0613
        self,
        button: disnake.ui.Button,
        interaction: disnake.MessageInteraction,
    ):
        # Decrements the embed count.
        self.embed_count -= 1

        # Gets the embed object.
        embed = self.embeds[self.embed_count]

        # Enables the next page button and disables the previous page button if we're on the first embed.
        self.next_page.disabled = False

        await interaction.response.edit_message(embed=embed, view=self)


    @disnake.ui.button(
        style=disnake.ButtonStyle.red,
        custom_id=f"persistent_view:nextpage_{str(disnake.Member)}awfawwa",
        label=f"🇳 🇪 🇽 🇹",
        row=4
    )
    async def next_page(
        self,
        button: disnake.ui.Button,
        interaction: disnake.MessageInteraction,
    ):
        # Checks if self.embed_count is within the valid range
        if 0 <= self.embed_count < len(self.embeds):
            # Increments the embed count
            self.embed_count += 1

            # Gets the embed object
            embed = self.embeds[self.embed_count]

            # Enables the previous page button and disables the next page button if we're on the last embed
            self.prev_page.disabled = False
            if self.embed_count == len(self.embeds) - 1:
                self.next_page.disabled = True

            await interaction.response.edit_message(embed=embed, view=self)


class PageSelect(disnake.ui.Select):
    def __init__(self, embeds: List[disnake.Embed]):
        # Create options using the title of each embed and first 4 characters of the description for context
        options = [
            disnake.SelectOption(
                label=f"{embed.title[:10]}: {embed.fields[:4]}...",  # Title + first 4 chars of the description
                value=str(i)  # String representation of the index
            ) for i, embed in enumerate(embeds)
        ]

        # Initialize the Select menu with the options
        super().__init__(
            custom_id="page_selector1",
            placeholder="Select a page...",
            min_values=1,
            max_values=1,
            options=options,
            row=0
        )
        
        self.embeds = embeds

    async def callback(self, interaction: disnake.Interaction):
        # Update the message with the selected embed
        await interaction.response.edit_message(embed=self.embeds[int(self.values[0])])