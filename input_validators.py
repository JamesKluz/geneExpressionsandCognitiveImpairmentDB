#Checks to make sure user input is an integer or a string 
#representing an integer and prompts user to re-enter
#until they have entered an integer value and then returns
#the value as an integer
def isInt(user_input):
  while True:
    try: 
      ui_int_val = int(user_input)
      break
    except ValueError:
      user_input = input("please enter an integer value >> ")
  return ui_int_val

#similar to the above function. Repeatedly prompts the user
#to enter either y for yes or n for no and then returns the
#input
def isYorN(user_input):
  while user_input != 'y' and user_input != 'n':
    user_input = input('Please enter either y for "yes" or n for "no" >> ')
  return user_input