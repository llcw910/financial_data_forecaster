from pybrain.datasets import SupervisedDataSet
from pybrain.supervised.trainers import BackpropTrainer
from pybrain.structure import FeedForwardNetwork
from pybrain.structure import LinearLayer, SigmoidLayer
from pybrain.structure import FullConnection
import json
import numpy as np
import Normalization as Norm
import matplotlib.pyplot as plt



with open('input.json') as input_file:
    json_obj = json.load(input_file)

input = []
output = []
dataSetSize = 500
Day = 26
rangeBegin = np.random.randint(50,len(json_obj['00001'])-dataSetSize-Day)
#for index in range(len(json_obj['00001'])-1):
for index in range(rangeBegin,rangeBegin+dataSetSize):
    dataPoint = json_obj['00001'][index]
    inputDataPoint = []
    if 'RSI_14' in dataPoint and \
       'D_14'   in dataPoint and \
       'K_14'   in dataPoint and \
       'MACD_signal_26_12_9' in dataPoint and \
       'EMA_14' in dataPoint:
           inputDataPoint.append(dataPoint['RSI_14'])
           inputDataPoint.append(dataPoint['D_14'])
           inputDataPoint.append(dataPoint['K_14'])
           inputDataPoint.append(dataPoint['EMA_14'])
           inputDataPoint.append(dataPoint['MACD_signal_26_12_9'])
           input.append(np.array(inputDataPoint))
           output.append(np.array([float(json_obj['00001'][index+Day]['close'])-float(json_obj['00001'][index]['close'])]))



Input = np.vstack(input)
Output = np.vstack(output)

forPlot = Output

norm = Norm.Normalization(Input, Output)

Input = norm.FeatureScaling(Input)
Output = norm.FeatureScaling(Output)

ds = SupervisedDataSet(5,1)
for (inp,outp) in zip(Input, Output):
    ds.addSample(inp,outp)

n = FeedForwardNetwork()
inLayer = LinearLayer(5)
hiddenLayer = SigmoidLayer(3)
outLayer = LinearLayer(1)
n.addInputModule(inLayer)
n.addModule(hiddenLayer)
n.addOutputModule(outLayer)
in_to_hidden = FullConnection(inLayer, hiddenLayer)
hidden_to_out = FullConnection(hiddenLayer, outLayer)
n.addConnection(in_to_hidden)
n.addConnection(hidden_to_out)
n.sortModules()

trainer = BackpropTrainer(n, ds)
for i in range(10000):
    err = trainer.train()
    print(err)
    
    

NNOutput=[]
for valIn in Input:
    NNOutput.append(n.activate(valIn))
    
NNOutput = np.vstack(NNOutput)
fig, ax = plt.subplots()
ax.plot(forPlot)
ax.plot(norm.FSOutDenorm(NNOutput))
plt.show()

print("-----Manual validation-----")
input = []
output = []
rangeBegin = np.random.randint(50,len(json_obj['00001'])-dataSetSize-Day)
print(rangeBegin)
for index in range(rangeBegin,rangeBegin+dataSetSize):
    dataPoint = json_obj['00001'][index]
    inputDataPoint = []
    if 'RSI_14' in dataPoint and \
       'D_14'   in dataPoint and \
       'K_14'   in dataPoint and \
       'MACD_signal_26_12_9'   in dataPoint and \
       'EMA_14' in dataPoint:
           inputDataPoint.append(dataPoint['RSI_14'])
           inputDataPoint.append(dataPoint['D_14'])
           inputDataPoint.append(dataPoint['K_14'])
           inputDataPoint.append(dataPoint['EMA_14'])
           inputDataPoint.append(dataPoint['MACD_signal_26_12_9'])
           input.append(np.array(inputDataPoint))
           output.append(np.array([float(json_obj['00001'][index+Day]['close'])-float(json_obj['00001'][index]['close'])]))

valInput = np.vstack(input)
valOutput = np.vstack(output)

valnorm = Norm.Normalization(valInput, valOutput)

valInput = valnorm.FeatureScaling(valInput)

valNNOutput=[]
for valIn in valInput:
    valNNOutput.append(n.activate(valIn))

valNNOutput = np.vstack(valNNOutput)
fig2, bx = plt.subplots()
bx.plot(valOutput)
bx.plot(norm.FSOutDenorm(valNNOutput))
plt.show()