# Example 1

from search import search_address

result_dict = search_address('РОССИЯ, 115522, Москва г, Пролетарский пр-кт, дом 17, корпус 1, квартира 160')

print(result_dict['normalized_address'])
print(result_dict['kladr'])

# Example 2

from search.address import Address

address = Address('РОССИЯ, 115522, Москва г, Пролетарский пр-кт, дом 17, корпус 1, квартира 160')
address.standardize()

print(address.to_dict()) # same as result dict
