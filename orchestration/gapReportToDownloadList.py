import json
import logging
import sys
from pathlib import Path

log = logging.getLogger()
log.setLevel(logging.INFO)
log.addHandler(logging.StreamHandler(sys.stdout))

if __name__ == "__main__":
    if not Path(sys.argv[1]).exists:
        raise RuntimeError(f'Input path: {sys.argv[1]} does not exist')
    
    with open(sys.argv[1]) as input:
        jsonInput = json.load(input)

        log.info(f'Found {len(jsonInput["unmatchedGroups"].keys())} unmatched products')
        log.info(f'Writing output to {sys.argv[2]}')

        with open(sys.argv[2], 'w') as output:
            for unmatched in jsonInput['unmatchedGroups']:
                for product in jsonInput["unmatchedGroups"][unmatched]['unmatched']:
                    output.write(f'{product}\n')
        
