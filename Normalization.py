# -*- coding: utf-8 -*-
"""
Created on Fri Jan  8 11:28:25 2016

@author: Keisei
"""

import numpy as np

class Normalization:
    
    Input = []
    Output = []
    InMax = []
    InMin = []
    OutMax = []
    OutMin = []
    
    def __init__(self, input, output):
        self.Input = input
        self.Output = output
        for inputType in self.Input.T:
            self.InMax.append(inputType.max())
            self.InMin.append(inputType.min())
        for outputType in self.Output.T:
            self.OutMax.append(outputType.max())
            self.OutMin.append(outputType.min())
        
        
    def FeatureScaling(self, input):
        inputSize = input.shape[1]
        output = []
        #print("normalizing size {0}".format(inputSize))
        for inputType in input.T:
            inMax = inputType.max()
            inMin = inputType.min()
            out = []
            for x in inputType:
                y = (x - inMin)/(inMax - inMin)
                out.append(y)
            output.append(out)
        return np.vstack(output).T
        
    def FSOutDenorm(self, input):
        inputSize = input.shape[1]
        output = []
        #print("denormalizeing size {0}".format(inputSize))
        for index in range(input.T.shape[0]):
            inputType = input.T[index]
            out = []
            for x in inputType:
                y = x*(self.OutMax[index] - self.OutMin[index]) + self.OutMin[index]
                out.append(y)
            output.append(out)
        return np.vstack(output).T
            
            
            