from collections import defaultdict
import re
from functools import reduce

def read_words_from_file(filename):
    """Reads words from a file, removes punctuation, and returns a list of words."""
    with open(filename, 'r') as file:
        text = file.read().lower()
        text = re.sub(r'[^\w\s]', '', text)
        return text.split()

def map_function(word):
    """Maps each word to a key-value pair (word, 1)."""
    return (word, 1)

def reduce_function(word_counts, pair):
    """Reduces word counts by summing occurrences."""
    word, count = pair
    word_counts[word] += count
    return word_counts

def map_reduce_word_count(words):
    """Simulates MapReduce for word counting."""
    # Map phase
    mapped_data = list(map(map_function, words))

    # Reduce phase
    word_count = reduce(reduce_function, mapped_data, defaultdict(int))

    return word_count

# Using MapReduce

filename = '/Users/rileymckenzie/Desktop/2025-semester/Principles/lab2.2/words.txt'  # Ensure the file is in the same directory, or use the full path.

words = read_words_from_file(filename)

word_count = map_reduce_word_count(words)

# Print the word counts
for word, count in word_count.items():
    print(f"'{word}': {count}")
