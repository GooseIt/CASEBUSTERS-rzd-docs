import os
import re
import logging
import json

import fitz  # PyMuPDF

from template import template # local import

INPUT_DIR = "../Files_in"
OUTPUT_DIR = "../Dicts_in"

def setup_logging() -> None:
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.INFO)


def main() -> None:
    template_block_lines: list[list[str]] = []
    file_constants: list[str] = []

    for i, template_block in enumerate(template):
        template_block_lines.append([])
        file_constants.append('')
        for line in template_block['lines']:
            for span in line['spans']:
                template_block_lines[i].append(span['text'])
        file_constants[i] = '\n'.join(template_block_lines[i])

    setup_logging()
    base_path = INPUT_DIR # TODO - в оригинале C11
    for path_name in os.listdir(base_path):
        mypath = f'{base_path}/{path_name}'

        logging.info('processing document %s', path_name)

        def parse_pdf(pdf_path: str) -> dict[float, str]:
            parsed_dict: dict[float, str] = {}
            with fitz.open(pdf_path) as doc:
                template_block_index = 0
                for page in doc:
                    blocks = page.get_text('dict')['blocks']
                    print(blocks)

                    for block_index, block in enumerate(blocks):
                        full_joined_text = '\n'.join([span['text'] for line in block['lines'] for span in line['spans']])

                        block_lines = []
                        for line in block['lines']:
                            for span in line['spans']:
                                block_lines.append(span['text'])

                        bad_example = True
                        for i, file_constant in enumerate(file_constants[template_block_index:]):
                            template_block_index_ = template_block_index + i
                            bad_example = True

                            logging.info('  [full] %s | %s', file_constant, full_joined_text)
                            for end in range(len(block_lines), 0, -1):
                                for start in range(0, len(block_lines), end):
                                    print('start', start, end)
                                    joined_text = '\n'.join([span['text'] for line in block['lines'][start:end] for span in line['spans']])
                                    if re.fullmatch(file_constant, joined_text):
                                        bad_example = False

                                        if template_block_index_ >= len(template):
                                            continue
                                        for current_line, template_line in zip(block['lines'], template[template_block_index_]['lines']):
                                            for span, template_span in zip(current_line['spans'], template_line['spans']):
                                                logging.info('    %s | %s', template_span['text'], span['text'])
                                                if span['size'] != template_span['size']:
                                                    logging.error('    incorrect size! %s ≠ %s', span['size'], template_span['size'], )
                                                    bad_example = True
                                                    break
                                                if span['color'] != template_span['color']:
                                                    logging.error('    incorrect color! %s ≠ %s', span['color'], template_span['color'])
                                                    bad_example = True
                                                    break
                                                if not re.fullmatch(template_span['text'], span['text']):
                                                    logging.error("    incorrect text! \"%s\" doesn't match to \"%s\"", span['text'], template_span['text'])
                                                    bad_example = True
                                                    break
                                                if 'save' in template_span:
                                                    parsed_dict[template_span['save']] = span['text']
                                            if bad_example:
                                                break
                                        if not bad_example:
                                            logging.error('  skipped %s template blocks', i)
                                            template_block_index = template_block_index_ + 1
                                            if block_index + 1 < len(blocks):
                                                if len(block['lines'][end:]) != 0:
                                                    print(len(block['lines'][end:] + blocks[block_index + 1]['lines']), 'vs', len(blocks[block_index + 1]['lines']))
                                                blocks[block_index + 1]['lines'] = block['lines'][end:] + blocks[block_index + 1]['lines']
                                            break
                                    if not bad_example:
                                        break
                                if not bad_example:
                                    break
                            if not bad_example:
                                break
                        if bad_example:
                            logging.error('  block with text \"%s\" skipped', full_joined_text)
            return parsed_dict
    
        parsed_pdf = parse_pdf(mypath)
        print(f'type: {type(parsed_pdf)}')
        with open(f'{OUTPUT_DIR}/{path_name[:-4]}_zachem.txt', "w") as f:
            f.write(str(parsed_pdf))


if __name__ == "__main__":
    main()

