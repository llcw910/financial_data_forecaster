# -*- coding: utf-8 -*-
"""
Created on Wed Dec 30 10:58:51 2015

@author: Keisei
"""

import numpy as np
import matplotlib.pyplot as plt

# Transfer functions
class TransferFunctions:
    def sgm(x, Derivative=False):
        if not Derivative:
            return 1.0 / (1.0 + np.exp(-x))
        else:
            out = 1.0 / (1.0 + np.exp(-x))
            return out * (1.0 - out)
    
    def linear(x, Derivative=False):
        if not Derivative:
            return x
        else:
            return 1.0
    
    def gaussian(x, Derivative=False):
        if not Derivative:
            return np.exp(-x**2)
        else:
            return -2*x*np.exp(-x**2)
    
    def tanh(x, Derivative=False):
        if not Derivative:
            return np.tanh(x)
        else:
            return 1.0 - np.tanh(x)**2
    
    def truncLinear(x, Derivative=False):
        if not Derivative:
            y = x.copy()
            y[y < 0] = 0
            return y
        else:
            return 1.0

class BackpropagationNetwork:
    
    #
    # Class members
    #
    layerCount = 0
    shape = None
    weights = []
    transFuncs = []
    
    #
    # Class methods
    #
    def __init__(self, layerSize, layerFunctions=None):
        
        # layer info
        self.layerCount = len(layerSize) - 1
        self.shape = layerSize
        
        if layerFunctions is None:
            lFuncs = []
            for i in range(self.layerCount):
                if i == self.layerCount - 1: 
                    lFuncs.append(TransferFunctions.linear)
                else:
                    lFuncs.append(TransferFunctions.sgm)
        else:
            if len(layerFunctions) != len(layerSize):
                raise ValueError("Incompatible length")
            elif layerFunctions[0] is not None:
                raise ValueError("Input cannot have transfer function")
            else:
                lFuncs = layerFunctions[1:]
        
        self.transFuncs = lFuncs
        
        # Data from last run
        self._layerInput = []
        self._layerOutput = []
        self._prevWeightDelta = []
        
        
        # Create the weight arrays
        for (l1,l2) in zip (layerSize[:-1],layerSize[1:]):
            self.weights.append(np.random.normal(scale=0.1, size = (l2, l1+1))) 
            self._prevWeightDelta.append(np.zeros([l2, l1+1]))

    # Run method
    def Run(self, input):
        """Run network based on the input data"""
        
        # Number of cases (number of rows of input)
        lnCases = input.shape[0]
    
        # Clear out the intermediate value lists
        self._layerInput = []
        self._layerOutput = []
        
        # Run
        for index in range(self.layerCount):
            if index == 0:
                layerInput = self.weights[0].dot(np.vstack([input.T, np.ones([1,lnCases])]))
            else:
                layerInput = self.weights[index].dot(np.vstack([self._layerOutput[-1], np.ones([1,lnCases])]))
                
            self._layerInput.append(layerInput)
            self._layerOutput.append(self.transFuncs[index](layerInput))
        
        return self._layerOutput[-1].T
    
    def RunVal(self, input, target):
        # run the network
        res = self.Run(input)
        # Compare to the target value                
        out_delta = self._layerOutput[self.layerCount - 1] - target.T
        error = np.sum(out_delta**2)
        
        return [res, error]
        
        
    # Train epoch
    def TrainEpoch(self, input, target, trainingRate = 0.2, momentum = 0.5):
        
        delta = []
        lnCases = input.shape[0]
        
        # run the network
        self.Run(input)
        
        # Calculate delta
        for index in reversed(range(self.layerCount)):
            if index == self.layerCount - 1:
                # Compare to the target value                
                out_delta = self._layerOutput[index] - target.T
                error = np.sum(out_delta**2)
                delta.append(out_delta * self.transFuncs[index](self._layerInput[index],True))
            else:
                # Compare to the following layer's delta
                delta_pullback = self.weights[index + 1].T.dot(delta[-1])
                delta.append(delta_pullback[:-1, :] * self.transFuncs[index](self._layerInput[index],True))
                
        # Compute weight delta
        for index in range(self.layerCount):
            delta_index = self.layerCount - 1 - index
            
            if index == 0:
                layerOutput = np.vstack([input.T,np.ones([1,lnCases])])
            else:
                layerOutput = np.vstack([self._layerOutput[index - 1], np.ones([1, self._layerOutput[index - 1].shape[1]])])
                
            currentWeightDelta = np.sum(layerOutput[None,:,:].transpose(2,0,1) * delta[delta_index][None,:,:].transpose(2,1,0) ,axis = 0)
            
            weightDelta = trainingRate * currentWeightDelta + momentum * self._prevWeightDelta[index]
            self.weights[index] -= weightDelta
            self._prevWeightDelta[index] = weightDelta
        return error
    
#            
# Test with XOR
#
if __name__ == "__main__":
    lvInput = np.array([[0,0],
                        [1,1],
                        [0,1],
                        [1,0]])
    lvTarget = np.array([[0.00],
                         [0.00],
                         [1.00],
                         [1.00]])
                         
    lFuncs = [None, TransferFunctions.sgm, TransferFunctions.sgm, TransferFunctions.linear]
    bpn = BackpropagationNetwork((2,2,2,1),lFuncs)
    print(bpn.shape)
    print(bpn.weights)
    
    lnMax = 1000000
    lnErr = 1e-5
    for i in range(lnMax+1):
        err = bpn.TrainEpoch(lvInput, lvTarget, )
        if i % 2500 == 0:
            print("Iteration {0} Error: {1:0.6f}".format(i,err))
        if err <= lnErr:
            print("Minimum error reached at iteration {0}".format(i))
            break
        
    lvOutput = bpn.Run(lvInput)
    print("Input: {0}\nOutput: {1}".format(lvInput, lvOutput))