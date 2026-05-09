name = 'Abhendu'
age = 39
city = 'Kolkata'
student = False

print('Name:', name , '|' , 'Age:', age)
print('City:', city , '|', 'Student:', student)

print('-------------------------------------------------')

number = 100
print('Number:', number)
print('Octal Number:', oct(number))
print('Hexadecimal Number:', hex(number))
print('Binary Number:', bin(number))
print(number == oct(number) == hex(number) == bin(number) )  
print(100 == 0o144 == 0x64 == 0b1100100 )

print('-------------------------------------------------')

print(type(42),  type(3.14),  type('hello'),  type(True),  type(int(3.9)),  type(bool(0)),  type(float('2.5')),  type(str(100)) )

print('-------------------------------------------------')

input_value1 = input('Enter value 1 : ')
print('You entered:', input_value1)

input_value2 = input('Enter value 2 : ')
print('You entered:', input_value2)

print('Addition:', int(input_value1) + int(input_value2))
print('Subtraction:', int(input_value1) - int(input_value2))
print('Multiplication:', int(input_value1) * int(input_value2))
print('Division:', int(input_value1) / int(input_value2))

print('-------------------------------------------------')
