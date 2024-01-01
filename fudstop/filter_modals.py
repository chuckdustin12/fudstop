from disnake.ext import commands
import disnake
from disnake import TextInputStyle




class ThetaModal(disnake.ui.Modal):
    def __init__(self):
        super().__init__(title="Theta Filter")

        # Minimum theta input
        self.min_theta = disnake.ui.TextInput(
            label="Minimum Theta",
            placeholder="Enter minimum theta value",
            custom_id="min_theta",
            style=disnake.TextInputStyle.short
        )
        self.add_item(self.min_theta)

        # Maximum theta input
        self.max_theta = disnake.ui.TextInput(
            label="Maximum Theta",
            placeholder="Enter maximum theta value",
            custom_id="max_theta",
            style=disnake.TextInputStyle.short
        )
        self.add_item(self.max_theta)

    async def callback(self, inter: disnake.ModalInteraction):
        min_theta = self.min_theta.value
        max_theta = self.max_theta.value
        # Process the input values here
        await inter.response.send_message(f"Theta range: {min_theta} - {max_theta}", ephemeral=True)