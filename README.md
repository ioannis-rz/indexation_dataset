# 📄 Indexed CSV Search Tool

## 🔍 Overview
This script allows you to efficiently search through a CSV file using indexation. Instead of scanning the entire file line by line, it builds an index based on slot and tx_id.

## Memory usage
Memory usage is very low since the hash table is stored on disk, and a binary search is performed on this file

## Preprocess
When making the index from the dataset, it is necessary to sort the hash, not just use the order of insertion

