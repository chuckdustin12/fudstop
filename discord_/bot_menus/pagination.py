import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))


from typing import List
import disnake

class AlertMenus(disnake.ui.View):
    def __init__(self, embeds: List[disnake.Embed] = None, assistant_id=None, thread_id=None):
        
        super().__init__(timeout=None)
        self.assistant_id = assistant_id
        self.thread_id = thread_id
        self.embeds = embeds if embeds is not None else []
        self.embed_count = 0
        self.prev_page.disabled = True
        self.count = 0

        if self.embeds:
            for i, embed in enumerate(self.embeds):
                embed.set_footer(text=f"Page {i + 1} of {len(self.embeds)}")

        if self.embeds and assistant_id is not None and thread_id is not None:
            for i, embed in enumerate(self.embeds):
                embed.set_footer(text=f"Page {i + 1} of {len(self.embeds)} | thread: {thread_id} | assistant | {assistant_id}")  



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
        # Create options using the title of each embed
        options = [
            disnake.SelectOption(
                label=embed.title,  # Use the title of the embed as the label
                value=str(i)  # Keep the value as a string representation of the index
            ) for i, embed in enumerate(embeds)
        ]

        # Initialize the Select menu with the options
        super().__init__(placeholder="Choose a page...", min_values=1, max_values=1, options=options)

        super().__init__(
            custom_id="page_selector1",
            placeholder="Pages 1-25",
            min_values=1,
            max_values=1,
            options=options,
            row=0
        )
        
        self.embeds = embeds

    async def callback(self, interaction: disnake.Interaction):
        await interaction.response.edit_message(embed=self.embeds[int(self.values[0])])