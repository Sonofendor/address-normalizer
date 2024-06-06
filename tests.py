import pandas as pd
from search import process_batch
from datetime import datetime
from random import sample

batch_size = 100

if __name__ == '__main__':

    with open('data/test_search.csv', 'rt', encoding='utf-8') as f:
        addresses = list(map(lambda x: x.strip('\n').strip('"'), f.readlines()[1:]))

    random_addresses = sample(addresses, batch_size)

    start_dttm = datetime.now()
    # обрабатываем на всех доступных ядрах
    processed_addresses = process_batch(random_addresses)
    end_dttm = datetime.now()

    df = pd.DataFrame(
        [
            {
                'original': result['original_address'],
                'normalized_address': result['normalized_address'],
                'score': result['search_summary']['kladr_status']
            } for result in processed_addresses
        ]
    )

    print(f'Test is finished in {(end_dttm - start_dttm).total_seconds()} seconds')
    print(f'Total number of addresses: {len(df)}')
    print(df.groupby(['score']).count())
