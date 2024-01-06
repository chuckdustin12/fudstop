# Function 1
def add(a, b):
    """
    Returns the sum of two numbers.
    """
    return a + b
def determine_emoji(row):
    if row['price'] > row['strike']:
        return "🔥"  # Flame emoji for put skew
    elif row['price'] < row['strike']:
        return "🟢"  # Green circle emoji for call skew
    else:
        return "N/A"  # If neither condition is met
# Function 2
def subtract(a, b):
    """
    Returns the difference between two numbers.
    """
    return a - b

# Function 3
def multiply(a, b):
    """
    Returns the product of two numbers.
    """
    return a * b

# Function 4
def divide(a, b):
    """
    Returns the result of dividing two numbers.
    """
    if b == 0:
        return "Division by zero is not allowed"
    return a / b

# Function 5
def square(a):
    """
    Returns the square of a number.
    """
    return a * a

# Function 6
def cube(a):
    """
    Returns the cube of a number.
    """
    return a * a * a

# Function 7
def power(base, exponent):
    """
    Returns the result of raising a number to a power.
    """
    return base ** exponent

# Function 8
def absolute_value(num):
    """
    Returns the absolute value of a number.
    """
    return abs(num)

# Function 9
def is_even(num):
    """
    Checks if a number is even.
    """
    return num % 2 == 0

# Function 10
def is_odd(num):
    """
    Checks if a number is odd.
    """
    return num % 2 != 0

# Function 11
def is_prime(num):
    """
    Checks if a number is prime.
    """
    if num <= 1:
        return False
    for i in range(2, int(num ** 0.5) + 1):
        if num % i == 0:
            return False
    return True

# Function 12
def factorial(num):
    """
    Returns the factorial of a non-negative integer.
    """
    if num == 0:
        return 1
    return num * factorial(num - 1)

# Function 13
def fibonacci(n):
    """
    Generates a list of Fibonacci numbers up to the n-th term.
    """
    if n <= 0:
        return []
    elif n == 1:
        return [0]
    elif n == 2:
        return [0, 1]
    else:
        fib_sequence = [0, 1]
        while len(fib_sequence) < n:
            fib_sequence.append(fib_sequence[-1] + fib_sequence[-2])
        return fib_sequence

# Function 14
def sum_of_digits(num):
    """
    Returns the sum of the digits of a number.
    """
    return sum(int(digit) for digit in str(num))

# Function 15
def reverse_string(string):
    """
    Reverses a given string.
    """
    return string[::-1]

# Function 16
def is_palindrome(string):
    """
    Checks if a string is a palindrome.
    """
    return string == string[::-1]

# Function 17
def list_duplicates(lst, item):
    """
    Returns a list of indices where the given item appears in the list.
    """
    return [index for index, value in enumerate(lst) if value == item]

# Function 18
def count_elements(lst):
    """
    Returns a dictionary counting the occurrences of each element in the list.
    """
    return {item: lst.count(item) for item in set(lst)}

# Function 19
def find_max(lst):
    """
    Returns the maximum element in a list.
    """
    if not lst:
        return None
    max_value = lst[0]
    for item in lst:
        if item > max_value:
            max_value = item
    return max_value

# Function 20
def find_min(lst):
    """
    Returns the minimum element in a list.
    """
    if not lst:
        return None
    min_value = lst[0]
    for item in lst:
        if item < min_value:
            min_value = item
    return min_value

# Function 21
def remove_duplicates(lst):
    """
    Removes duplicates from a list and returns a new list.
    """
    return list(set(lst))

# Function 22
def average(lst):
    """
    Calculates the average of a list of numbers.
    """
    if not lst:
        return None
    return sum(lst) / len(lst)

# Function 23
def median(lst):
    """
    Calculates the median of a list of numbers.
    """
    if not lst:
        return None
    sorted_lst = sorted(lst)
    n = len(sorted_lst)
    if n % 2 == 0:
        mid1 = sorted_lst[n // 2 - 1]
        mid2 = sorted_lst[n // 2]
        return (mid1 + mid2) / 2
    else:
        return sorted_lst[n // 2]

# Function 24
def mode(lst):
    """
    Returns the mode(s) of a list of numbers.
    """
    if not lst:
        return None
    from collections import Counter
    counts = Counter(lst)
    max_count = max(counts.values())
    return [item for item, count in counts.items() if count == max_count]

# Function 25
def find_index(lst, item):
    """
    Returns the index of the first occurrence of an item in a list.
    """
    if item in lst:
        return lst.index(item)
    else:
        return -1

# Function 26
def merge_dicts(dict1, dict2):
    """
    Merges two dictionaries into a new dictionary.
    """
    merged_dict = dict1.copy()
    merged_dict.update(dict2)
    return merged_dict

# Function 27
def reverse_dict(dictionary):
    """
    Reverses the keys and values of a dictionary.
    """
    return {value: key for key, value in dictionary.items()}

# Function 28
def filter_list(lst, condition):
    """
    Filters a list based on a given condition.
    """
    return [item for item in lst if condition(item)]

# Function 29
def split_string(string, delimiter):
    """
    Splits a string into a list of substrings using a delimiter.
    """
    return string.split(delimiter)

# Function 30
def join_list(lst, delimiter):
    """
    Joins a list of strings into a single string using a delimiter.
    """
    return delimiter.join(lst)

# Function 31
def capitalize_words(string):
    """
    Capitalizes the first letter of each word in a string.
    """
    return ' '.join(word.capitalize() for word in string.split())

# Function 32
def count_vowels(string):
    """
    Counts the number of vowels in a string.
    """
    vowels = "aeiouAEIOU"
    return sum(1 for char in string if char in vowels)

# Function 33
def count_consonants(string):
    """
    Counts the number of consonants in a string.
    """
    consonants = "bcdfghjklmnpqrstvwxyzBCDFGHJKLMNPQRSTVWXYZ"
    return sum(1 for char in string if char in consonants)

# Function 34
def remove_whitespace(string):
    """
    Removes all whitespace characters from a string.
    """
    return ''.join(string.split())

# Function 35
def is_anagram(word1, word2):
    """
    Checks if two words are anagrams of each other.
    """
    return sorted(word1) == sorted(word2)

# Function 36
def to_binary(num):
    """
    Converts an integer to a binary string.
    """
    return bin(num)

# Function 37
def to_hexadecimal(num):
    """
    Converts an integer to a hexadecimal string.
    """
    return hex(num)

# Function 38
def to_octal(num):
    """
    Converts an integer to an octal string.
    """
    return oct(num)

# Function 39
def is_binary(string):
    """
    Checks if a string is a valid binary representation.
    """
    try:
        int(string, 2)
        return True
    except ValueError:
        return False

# Function 40
def is_hexadecimal(string):
    """
    Checks if a string is a valid hexadecimal representation.
    """
    try:
        int(string, 16)
        return True
    except ValueError:
        return False

# Function 41
def is_octal(string):
    """
    Checks if a string is a valid octal representation.
    """
    try:
        int(string, 8)
        return True
    except ValueError:
        return False

# Function 42
def to_lowercase(string):
    """
    Converts a string to lowercase.
    """
    return string.lower()

# Function 43
def to_uppercase(string):
    """
    Converts a string to uppercase.
    """
    return string.upper()

# Function 44
def strip_whitespace(string):
    """
    Removes leading and trailing whitespace from a string.
    """
    return string.strip()

# Function 45
def remove_punctuation(string):
    """
    Removes all punctuation characters from a string.
    """
    import string
    translator = str.maketrans('', '', string.punctuation)
    return string.translate(translator)

# Function 46
def rotate_string(string, n):
    """
    Rotates a string by a given number of positions.
    """
    if not string:
        return string
    n = n % len(string)
    return string[n:] + string[:n]

# Function 47
def flatten(lst):
    """
    Flattens a nested list into a single list.
    """
    if not lst:
        return []
    if isinstance(lst[0], list):
        return flatten(lst[0]) + flatten(lst[1:])
    return [lst[0]] + flatten(lst[1:])

# Function 48
def deep_copy(obj):
    """
    Creates a deep copy of an object.
    """
    import copy
    return copy.deepcopy(obj)

# Function 49
def is_subset(subset, lst):
    """
    Checks if one list is a subset of another list.
    """
    return all(item in lst for item in subset)

# Function 50
def is_superset(superset, lst):
    """
    Checks if one list is a superset of another list.
    """
    return all(item in superset for item in lst)

# Function 51
def list_intersection(lst1, lst2):
    """
    Returns the intersection of two lists.
    """
    return list(set(lst1) & set(lst2))

# Function 52
def list_union(lst1, lst2):
    """
    Returns the union of two lists.
    """
    return list(set(lst1) | set(lst2))

# Function 53
def list_difference(lst1, lst2):
    """
    Returns the difference between two lists.
    """
    return list(set(lst1) - set(lst2))

# Function 54
def list_symmetric_difference(lst1, lst2):
    """
    Returns the symmetric difference between two lists.
    """
    return list(set(lst1) ^ set(lst2))

# Function 55
def list_merge(lst1, lst2):
    """
    Merges two lists into a new list.
    """
    return lst1 + lst2

# Function 56
def list_split(lst, size):
    """
    Splits a list into smaller lists of a specified size.
    """
    return [lst[i:i + size] for i in range(0, len(lst), size)]

# Function 57
def list_reverse(lst):
    """
    Reverses a list.
    """
    return lst[::-1]

# Function 58
def list_sort(lst):
    """
    Sorts a list in ascending order.
    """
    return sorted(lst)

# Function 59
def list_shuffle(lst):
    """
    Shuffles the elements of a list randomly.
    """
    from random import shuffle
    shuffled_lst = lst.copy()
    shuffle(shuffled_lst)
    return shuffled_lst

# Function 60
def list_rotate(lst, n):
    """
    Rotates a list by a given number of positions.
    """
    n = n % len(lst)
    return lst[n:] + lst[:n]

# Function 61
def list_index_of(lst, item):
    """
    Returns the index of the first occurrence of an item in a list.
    """
    if item in lst:
        return lst.index(item)
    else:
        return -1

# Function 62
def list_remove_all(lst, item):
    """
    Removes all occurrences of an item from a list.
    """
    return [x for x in lst if x != item]

# Function 63
def list_unique_elements(lst):
    """
    Returns a list of unique elements from a list.
    """
    return list(set(lst))

# Function 64
def list_duplicate_elements(lst):
    """
    Returns a list of elements that appear more than once in a list.
    """
    unique_lst = list(set(lst))
    return [item for item in unique_lst if lst.count(item) > 1]

# Function 65
def list_first_n_elements(lst, n):
    """
    Returns the first n elements of a list.
    """
    return lst[:n]

# Function 66
def list_last_n_elements(lst, n):
    """
    Returns the last n elements of a list.
    """
    return lst[-n:]


def chunk_string(string, size):
    """Yield successive size-sized chunks from string."""
    for i in range(0, len(string), size):
        yield string[i:i + size]
# Function 67
def list_nth_element(lst, n):
    """
    Returns the n-th element of a list.
    """
    if 0 <= n < len(lst):
        return lst[n]
    else:
        return None

# Function 68
def list_random_element(lst):
    """
    Returns a random element from a list.
    """
    from random import choice
    return choice(lst)

# Function 69
def dict_keys(dictionary):
    """
    Returns a list of keys from a dictionary.
    """
    return list(dictionary.keys())

# Function 70
def dict_values(dictionary):
    """
    Returns a list of values from a dictionary.
    """
    return list(dictionary.values())

# Function 71
def dict_items(dictionary):
    """
    Returns a list of key-value pairs from a dictionary.
    """
    return list(dictionary.items())

# Function 72
def dict_length(dictionary):
    """
    Returns the number of key-value pairs in a dictionary.
    """
    return len(dictionary)


# Function 73
def dict_contains_key(dictionary, key):
    return key in dictionary

# Function 74
def dict_contains_value(dictionary, value):
    return value in dictionary.values()

# Function 75
def dict_get(dictionary, key, default=None):
    return dictionary.get(key, default)

# Function 76
def dict_set(dictionary, key, value):
    dictionary[key] = value

# Function 77
def dict_remove(dictionary, key):
    if key in dictionary:
        del dictionary[key]

# Function 78
def dict_clear(dictionary):
    dictionary.clear()

# Function 79
def dict_copy(dictionary):
    return dictionary.copy()

# Function 80
def dict_merge(dict1, dict2):
    merged_dict = dict1.copy()
    merged_dict.update(dict2)
    return merged_dict

# Function 81
def dict_reverse(dictionary):
    return {value: key for key, value in dictionary.items()}

# Function 82
def dict_filter(dictionary, condition):
    return {key: value for key, value in dictionary.items() if condition(key, value)}

# Function 83
def dict_sorted_keys(dictionary):
    return sorted(dictionary.keys())

# Function 84
def dict_sorted_values(dictionary):
    return [value for key, value in sorted(dictionary.items())]

# Function 85
def dict_sorted_items(dictionary):
    return [(key, value) for key, value in sorted(dictionary.items())]

# Function 86
def dict_sorted_by_key(dictionary, reverse=False):
    return dict(sorted(dictionary.items(), key=lambda x: x[0], reverse=reverse))

# Function 87
def dict_sorted_by_value(dictionary, reverse=False):
    return dict(sorted(dictionary.items(), key=lambda x: x[1], reverse=reverse))

# Function 88
def set_add(s, item):
    s.add(item)

# Function 89
def set_remove(s, item):
    s.remove(item)

# Function 90
def set_discard(s, item):
    s.discard(item)

# Function 91
def set_clear(s):
    s.clear()

# Function 92
def set_copy(s):
    return s.copy()

# Function 93
def set_union(s1, s2):
    return s1 | s2

# Function 94
def set_intersection(s1, s2):
    return s1 & s2

# Function 95
def set_difference(s1, s2):
    return s1 - s2

# Function 96
def set_symmetric_difference(s1, s2):
    return s1 ^ s2

# Function 97
def set_subset(s1, s2):
    return s1.issubset(s2)

# Function 98
def set_superset(s1, s2):
    return s1.issuperset(s2)

# Function 99
def set_length(s):
    return len(s)

# Function 100
def set_contains(s, item):
    return item in s
