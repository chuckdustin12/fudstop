#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import torch
from datasets import load_dataset
from transformers import (
    GPT2Tokenizer,
    GPT2LMHeadModel,
    DataCollatorForLanguageModeling,
    TrainingArguments,
    Trainer
)

def main():
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Using device: {device}")

    # 1. Load GPT-2 tokenizer
    tokenizer = GPT2Tokenizer.from_pretrained("gpt2")
    tokenizer.pad_token = tokenizer.eos_token

    # 2. Load JSONL dataset
    dataset_path = "messages.jsonl"
    # Replace "all" with a different key, e.g. "data"
    raw_dataset = load_dataset("json", data_files={"data": dataset_path})

    # 3. Access your dataset split
    # e.g. if you want to do a train/validation split
    dataset = raw_dataset["data"].train_test_split(test_size=0.1, seed=42)
    train_dataset = dataset["train"]
    eval_dataset = dataset["test"]

    # 4. Tokenize function
    def tokenize_function(example):
        return tokenizer(
            example["text"],
            truncation=True,
            padding="max_length",
            max_length=512
        )

    # 5. Tokenize
    train_dataset = train_dataset.map(tokenize_function, batched=True, remove_columns=["text"])
    eval_dataset = eval_dataset.map(tokenize_function, batched=True, remove_columns=["text"])

    # 6. Data collator
    data_collator = DataCollatorForLanguageModeling(
        tokenizer=tokenizer,
        mlm=False
    )

    # 7. Load model
    model = GPT2LMHeadModel.from_pretrained("gpt2").to(device)

    # 8. Training arguments
    training_args = TrainingArguments(
        output_dir="./results",
        num_train_epochs=2,
        per_device_train_batch_size=1,
        warmup_steps=500,
        weight_decay=0.01,
        logging_dir="./logs",
        remove_unused_columns=True
    )

    # 9. Trainer
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=eval_dataset,
        data_collator=data_collator
    )

    # 10. Train
    trainer.train()

    # 11. Save
    model_dir = "./llms"
    os.makedirs(model_dir, exist_ok=True)
    model.save_pretrained(model_dir)
    tokenizer.save_pretrained(model_dir)
    print(f"Model and tokenizer saved in: {model_dir}")

if __name__ == "__main__":
    main()
