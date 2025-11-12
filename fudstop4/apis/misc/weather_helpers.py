def kelvin_to_temp(kelvin_temp, unit="C"):
    if unit == "F":
        return (kelvin_temp - 273.15) * 9/5 + 32
    elif unit == "C":
        return kelvin_temp - 273.15
    else:
        raise ValueError("Invalid unit. Please use 'C' for Celsius or 'F' for Fahrenheit.")

# Using the function: