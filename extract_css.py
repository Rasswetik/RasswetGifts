
import re

try:
    with open('E:\\RasswetGifts\\giftsbattle\\js\\index.C0epwQna.js', 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()

    print('--- Glow Object ---')
    # Search for "glow:{" and capture until matching brace
    # Simple counting of braces
    match = re.search(r'glow:\{', content)
    if match:
        start = match.start()
        brackets = 0
        end = start
        found = False
        for i in range(start, len(content)):
            if content[i] == '{': brackets += 1
            elif content[i] == '}': brackets -= 1
            
            if brackets == 0:
                end = i + 1
                found = True
                break
        
        if found:
            print(content[start:end])
        else:
            print("Could not find end of object")
            print(content[start:start+500])

except Exception as e:
    print(e)
