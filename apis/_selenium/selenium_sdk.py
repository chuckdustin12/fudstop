from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
def take_screenshot(url, file_name):
    """
    Opens the URL in a headless Chrome, clicks the specified button, selects the stars, and takes a screenshot.
    :param url: URL of the webpage to take a screenshot of
    :param file_name: Name of the file to save the screenshot
    :param chrome_driver_path: Path to the chromedriver executable
    """
    # Chrome options for headless browser
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-web-security")

    # Initialize the driver
    driver = webdriver.Chrome(options=chrome_options)
    driver.implicitly_wait(10)  # Wait for elements to be present

    try:
        # Open the URL
        driver.get(url)

        # Wait for the button to be clickable and click it
        button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button#ctl00_ContentPlaceHolder1_ctl02_Button1"))
        )
        button.click()

        # Wait for the stars to be clickable and click the third star
        third_star = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "span.d-none.d-lg-inline + span + span"))
        )
        third_star.click()

        # Take the screenshot
        driver.save_screenshot(file_name)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        driver.quit()

# Example usage
take_screenshot('https://tradingeconomics.com/calendar', 'screenshot.png')