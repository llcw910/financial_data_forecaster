#
# Neural Network Trainer Class
#
import numpy as np
import NeuralNetwork as NN
import Normalization as Norm
import json
import matplotlib.pyplot as plt
import datetime

#
# Define the Trainer class
#
class NetworkTrainer:
    """This is a class which trains neural networks"""
    
    #
    # Class Members
    #
    Network = None
    DataSet = []
    MaxIterations = 300000
    
    #
    # Class methods
    #
    def __init__(self, network, dataSet):
        """Set up the trainer class"""
        
        if not isinstance(network, NN.BackpropagationNetwork):
            raise ValueError("Bad parameter {network}")
        
        self.Network = network
        self.DataSet = dataSet
    
    def TrainNetwork(self, min_error=0.1, display=True, Forever=False, **kwags):
        """Train the network until a specific error is reached"""
        if Forever:
            i = 0
            while True:
                error = bpn.TrainEpoch(self.DataSet[0], self.DataSet[1], **kwags)
                if error <= min_error:
                    break
                if error in [np.nan, np.inf]:
                    print("An error occurred during training.")
                    break
                if display and i % 20000 == 0:
                    print("Iteration {0:9d} Error: {1:0.6f}".format(i, error))
                    NNOutput = bpn.Run(self.DataSet[0])
                    fig, ax = plt.subplots()
                    ax.plot(norm.FSOutDenorm(self.DataSet[1]))
                    ax.plot(norm.FSOutDenorm(NNOutput))
                    plt.show()
                i+=1
            return i
        else:
            for i in range(self.MaxIterations):
                error = bpn.TrainEpoch(self.DataSet[0], self.DataSet[1], **kwags)
                if error <= min_error:
                    break
                if error in [np.nan, np.inf]:
                    print("An error occurred during training.")
                    break
                if display and i % 10000 == 0:
                    print("Iteration {0:9d} Error: {1:0.6f}".format(i, error))
            
            return i

#
# Run main
#
if __name__ == "__main__":
    
    # Create the network
    bpn = NN.BackpropagationNetwork((5,3,1), [None, NN.TransferFunctions.sgm, NN.TransferFunctions.linear])
    
    # Create the data set
    input = []
    output = []
    
    with open('input.json') as input_file:
        json_obj = json.load(input_file)
        
    print(len(json_obj['00001']))
    
    dataSetSize = 200
    Day = 20
    rangeBegin = np.random.randint(50,len(json_obj['00001'])-dataSetSize-Day)
    #for index in range(len(json_obj['00001'])-1):
    for index in range(rangeBegin,rangeBegin+dataSetSize):
        dataPoint = json_obj['00001'][index]
        inputDataPoint = []
        if 'RSI_14' in dataPoint and \
           'D_14'   in dataPoint and \
           'K_14'   in dataPoint and \
           'MACD_signal_26_12_9'   in dataPoint and \
           'EMA_14' in dataPoint:
               inputDataPoint.append(dataPoint['RSI_14'])
               #inputDataPoint.append(dataPoint['UP_14'])
               inputDataPoint.append(dataPoint['D_14'])
               inputDataPoint.append(dataPoint['K_14'])
               #inputDataPoint.append(dataPoint['DOWN_14'])
               inputDataPoint.append(dataPoint['EMA_14'])
               inputDataPoint.append(dataPoint['MACD_signal_26_12_9'])
               input.append(np.array(inputDataPoint))
               output.append(np.array([float(json_obj['00001'][index+Day]['close'])]))
    
    Input = np.vstack(input)
    Output = np.vstack(output)
    
    forPlot = Output
    
    norm = Norm.Normalization(Input, Output)
    
    Input = norm.FeatureScaling(Input)
    Output = norm.FeatureScaling(Output)

    # Train it!
    trn = NetworkTrainer(bpn, [Input, Output])
    iter = trn.TrainNetwork(1e-4, True, Forever = False, trainingRate = 0.01, momentum = 0.75)
    
    # Run the network on trained data set
    NNOutput = bpn.Run(Input)
    
#    for i in range(Input.shape[0]):
#        print("Input {0} Output {1}".format(Input[i], np.round(Output[i], 2)))
    
    print(bpn.weights[0])
    print(bpn.weights[1])
    #print(Output)
    #print(NNOutput)
    
    fig, ax = plt.subplots()
    ax.plot(forPlot)
    ax.plot(norm.FSOutDenorm(NNOutput))
    plt.show()
    
    
    #Test manual validation
    print("-----Manual validation-----")
    input = []
    output = []
    rangeBegin = np.random.randint(50,len(json_obj['00001'])-dataSetSize-Day)
    for index in range(rangeBegin,rangeBegin+dataSetSize):
        dataPoint = json_obj['00001'][index]
        inputDataPoint = []
        if 'RSI_14' in dataPoint and \
           'D_14'   in dataPoint and \
           'K_14'   in dataPoint and \
           'MACD_signal_26_12_9'   in dataPoint and \
           'EMA_14' in dataPoint:
               inputDataPoint.append(dataPoint['RSI_14'])
               #inputDataPoint.append(dataPoint['UP_14'])
               inputDataPoint.append(dataPoint['D_14'])
               inputDataPoint.append(dataPoint['K_14'])
               #inputDataPoint.append(dataPoint['DOWN_14'])
               inputDataPoint.append(dataPoint['EMA_14'])
               inputDataPoint.append(dataPoint['MACD_signal_26_12_9'])
               input.append(np.array(inputDataPoint))
               output.append(np.array([float(json_obj['00001'][index+Day]['close'])]))

    valInput = np.vstack(input)
    valOutput = np.vstack(output)
    
    valnorm = Norm.Normalization(valInput, valOutput)
    
    valInput = valnorm.FeatureScaling(valInput)
    
    valNNOutput = bpn.Run(valInput)
    
    fig2, bx = plt.subplots()
    bx.plot(valOutput)
    bx.plot(norm.FSOutDenorm(valNNOutput))
    plt.show()