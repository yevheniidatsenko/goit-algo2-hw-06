import requests
import re
import multiprocessing
import matplotlib.pyplot as plt
from collections import Counter
from colorama import Fore, Style, init

# Initialize colorama for colored output
init(autoreset=True)

# Function to fetch text from a given URL
def fetch_text(url):
    try:
        # Send a GET request to the URL
        response = requests.get(url)
        # Raise an exception for bad status codes
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        # Print error message if request fails
        print(Fore.RED + f"Error fetching text: {e}")
        return ""

# Function to tokenize the text into words
def tokenize(text):
    # Use regular expression to find all words in the text
    words = re.findall(r"\b\w+\b", text.lower())
    return words

# Function to map each chunk of words to a Counter
def mapper(words):
    # Create a Counter object with each word mapped to its count
    return Counter(words)

# Function to reduce the mapped counts into a single Counter
def reducer(counters):
    # Initialize an empty Counter
    counter = Counter()
    # Add up all the counts from the mapped results
    for c in counters:
        counter += c
    return counter

# Function to perform MapReduce on the text
def map_reduce(text, num_workers=4):
    # Tokenize the text into words
    words = tokenize(text)
    # Calculate the chunk size for parallel processing
    chunk_size = len(words) // num_workers
    # Split the words into chunks
    chunks = [words[i:i + chunk_size] for i in range(0, len(words), chunk_size)]
    
    # Use multiprocessing to map each chunk in parallel
    with multiprocessing.Pool(num_workers) as pool:
        mapped = pool.map(mapper, chunks)
    
    # Reduce the mapped results
    return reducer(mapped)

# Function to visualize the top words by frequency
def visualize_top_words(word_counts, top_n=10):
    # Get the top N words by frequency
    top_words = word_counts.most_common(top_n)
    # Separate words and counts
    words, counts = zip(*top_words)
    # Create a bar chart
    plt.figure(figsize=(10, 5))
    plt.bar(words, counts, color="skyblue")
    plt.xlabel("Words")
    plt.ylabel("Frequency")
    plt.title("Top Words by Frequency")
    plt.xticks(rotation=45)
    plt.show()

# Function to print the top words by frequency
def print_top_words(word_counts, top_n=10):
    # Print header
    print(Fore.CYAN + Style.BRIGHT + "Top Words by Frequency:\n")
    # Print each top word with its frequency
    for i, (word, count) in enumerate(word_counts.most_common(top_n), 1):
        print(Fore.YELLOW + f"{i}. {word}: " + Fore.GREEN + f"{count}")

if __name__ == "__main__":
    # URL to fetch text from
    url = "https://www.gutenberg.org/files/1342/1342-0.txt"
    # Fetch the text
    text = fetch_text(url)
    if text:
        # Perform MapReduce on the text
        word_counts = map_reduce(text)
        # Print and visualize the top words
        print_top_words(word_counts)
        visualize_top_words(word_counts)