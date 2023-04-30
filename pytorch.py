import torch
import torch.nn as nn
import numpy as np
from torch.utils.data import DataLoader, Dataset
from sklearn.model_selection import train_test_split
import pandas as pd
import time
start_time = time.time()
#%%
# our data
gopro = pd.read_csv('Clean_Final/gopro_final_data.csv')
gopro_x = gopro.drop(['week','highChange (above 6% change)', 'aboveAvgVol'], axis=1).values
# print(gopro_x)
gopro_y1 = gopro['highChange (above 6% change)'].values

# split data into train and test
gopro_x_train, gopro_x_test, gopro_y1_train, gopro_y1_test = train_test_split(gopro_x, gopro_y1, test_size=0.2, random_state=42)
#%%
class Data(Dataset):
    def __init__(self, X_train, y_train):
        # need to convert float64 to float32 else
        # will get the following error
        # RuntimeError: expected scalar type Double but found Float
        self.X = torch.from_numpy(X_train.astype(np.float32))
        # need to convert float64 to Long else
        # will get the following error
        # RuntimeError: expected scalar type Long but found Float
        self.y = torch.from_numpy(y_train).type(torch.LongTensor)
        self.len = self.X.shape[0]

    def __getitem__(self, index):
        return self.X[index], self.y[index]

    def __len__(self):
        return self.len


# our data
gopro_train = Data(gopro_x_train, gopro_y1_train)
batch_size = 4
gopro_loader = DataLoader(gopro_train, batch_size=batch_size,
                         shuffle=True)
#%%
import torch.nn as nn
# number of features (len of X cols)
input_dim = 22
# number of hidden layers
hidden_layers = 25
# number of classes (unique of y)
output_dim = 3
class Network(nn.Module):
  def __init__(self):
    super(Network, self).__init__()
    self.linear1 = nn.Linear(input_dim, hidden_layers)
    self.linear2 = nn.Linear(hidden_layers, output_dim)
  def forward(self, x):
    x = torch.sigmoid(self.linear1(x))
    x = self.linear2(x)
    return x
#%%
clf = Network()
print(clf.parameters)
#%%
criterion = nn.CrossEntropyLoss()
optimizer = torch.optim.SGD(clf.parameters(), lr=0.001)
#%%
epoch = 2
for epoch in range(epoch):
    running_loss = 0
    for i, data in enumerate(gopro_loader, 0):
        inputs, labels = data
        # set optimizer to zero grad to remove previous epoch gradients
        optimizer.zero_grad()
        # forward propagation
        outputs = clf(inputs)
        loss = criterion(outputs, labels)
        # backward propagation
        loss.backward()
        # optimize
        optimizer.step()
        running_loss += loss.item()
    # display statistics
    print(f'[{epoch+ 1}, {i + 1:5d}] loss: {running_loss/ 2000:.5f}')
#%%
path = './gopro_model1.path'
torch.save(clf.state_dict(), path)
#%%
# test model
model = Network()
model.load_state_dict(torch.load(path))

# test data
gopro_test = Data(gopro_x_test, gopro_y1_test)
gopro_test_loader = DataLoader(gopro_test, batch_size=batch_size,
                        shuffle=True)

# test
correct, total = 0, 0
# no need to calculate gradients during inference
with torch.no_grad():
  for data in gopro_test_loader:
    inputs, labels = data
    # calculate output by running through the network
    outputs = model(inputs)
    # get the predictions
    __, predicted = torch.max(outputs.data, 1)
    print([predicted, labels])
    # update results
    total += labels.size(0)
    correct += (predicted == labels).sum().item()
print(f'Accuracy of the network on the {len(gopro_test)} test data: {100 * correct // total} %')
end_time = time.time()
print("Execution time: {:.2f} seconds".format(end_time - start_time))