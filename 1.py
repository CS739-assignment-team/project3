import pickle 

with open('state_4000.pickle', 'rb') as file:
    bytes = file.read()
    data_dict = pickle.loads(bytes)
    # data = pickle.load(file)
    print(data_dict)