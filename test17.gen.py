from transformers import GPT2Tokenizer, GPT2LMHeadModel

# Load the fine-tuned model and tokenizer
model_dir = "./llms"
tokenizer = GPT2Tokenizer.from_pretrained(model_dir)
model = GPT2LMHeadModel.from_pretrained(model_dir)

# Ensure the padding token is set correctly (GPT-2 does not have one by default)
tokenizer.pad_token = tokenizer.eos_token

def generate_text(prompt, max_length=100, num_return_sequences=1):
    """
    Generate text using the fine-tuned GPT-2 model.
    
    Args:
        prompt (str): The initial text prompt.
        max_length (int): The maximum number of tokens to generate.
        num_return_sequences (int): The number of outputs to return.

    Returns:
        list: A list of generated text outputs.
    """
    # Tokenize input
    inputs = tokenizer(prompt, return_tensors="pt")

    # Generate text
    outputs = model.generate(
        **inputs,
        max_length=max_length,
        num_return_sequences=num_return_sequences,
        do_sample=True,  # Enables randomness for diverse outputs
        top_k=50,  # Limits sampling pool to 50 highest probability tokens
        top_p=0.95,  # Uses nucleus sampling
        temperature=0.2,  # Adjusts randomness (lower = more deterministic)
    )

    # Decode and return the text outputs
    return [tokenizer.decode(output, skip_special_tokens=True) for output in outputs]

# Example usage
if __name__ == "__main__":
    while True:
        prompt = input("Enter a prompt: ")
        if prompt.lower() == "exit":
            break
        generated_texts = generate_text(prompt, max_length=150, num_return_sequences=1)
        print("\nGenerated Text:\n", generated_texts[0], "\n")