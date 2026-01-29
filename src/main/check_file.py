import json

with open('../../data/expenses_rename.json', 'r', encoding='utf-8') as f:
    file = json.load(f)


print(file)