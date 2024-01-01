import random

def generate_random_style():
    # Define lists of possible CSS property values
    colors = ["red", "blue", "green", "orange", "purple", "pink", "yellow"]
    font_sizes = ["12px", "16px", "20px", "24px", "28px"]
    margins = ["10px", "20px", "30px", "40px", "50px"]
    borders = ["1px solid black", "2px dashed gray", "3px dotted blue"]
    
    # Generate random CSS properties
    color = random.choice(colors)
    font_size = random.choice(font_sizes)
    margin = random.choice(margins)
    border = random.choice(borders)
    
    # Construct the CSS style string
    css_style = f"""
        background-color: {color};
        color: white;
        font-size: {font_size};
        margin: {margin};
        border: {border};
        padding: 10px;
        text-align: center;
        border-radius: 10px;
    """
    
    return css_style


def generate_random_navigation():
    # Generate a random navigation bar
    navigation_content = f"""
        <nav class="navbar">
            <ul>
                <li><a href="#">Home</a></li>
                <li><a href="#">About</a></li>
                <li><a href="#">Services</a></li>
                <li><a href="#">Portfolio</a></li>
                <li><a href="#">Contact</a></li>
            </ul>
        </nav>
    """
    
    return navigation_content

def generate_random_js():
    # Generate random JavaScript code
    js_code = f"""
        <script>
            // Random JavaScript code
            alert('Hello, this is random JavaScript!');
        </script>
    """
    
    return js_code

def generate_random_html():
    # Generate random CSS, navigation bar, JavaScript, and HTML
    random_style = generate_random_style()
    random_navigation = generate_random_navigation()
    random_js = generate_random_js()
    
    # Construct the HTML content
    html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Random HTML Page</title>
            <style>
                {random_style}
            </style>
        </head>
        <body>
            {random_navigation}
            {random_js}
        </body>
        </html>
    """
    
    # Save the HTML content to a file (e.g., 'random_nav.html')
    with open('random_nav.html', 'w') as html_file:
        html_file.write(html_content)

