
import csv
import numpy as np
import pandas as pd
from keras.models import Sequential
from keras.layers import Dense
from keras.models import model_from_json
from keras.utils import plot_model
from keras.callbacks import EarlyStopping
from sklearn.neural_network import MLPClassifier
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import classification_report, confusion_matrix


class SalesDataOneHotEncoder:
    def __init__(self, columnName, categoryArray):
        self.ohe = OneHotEncoder(categories=[categoryArray], handle_unknown='ignore');
        self.columnName = columnName;
        self.categoryArray = categoryArray;
        catIndex = 1;
        categories = [];
        for aCategory in categoryArray:
            catWithIndex = [aCategory, catIndex];
            categories.append(catWithIndex)
            catIndex = catIndex + 1;
        self.categories = categories;

oheList = [
        SalesDataOneHotEncoder('Opportunity_Type', ['Cloud Hyperscaler','Cloud Quoted','LenovoNetApp','OEM','Regular']),
        SalesDataOneHotEncoder('Forecast_Category_Text', ['Best Case','Closed','Committed','Omitted','Pipeline']),
        SalesDataOneHotEncoder('SalesUnityPartyName', ['0','Americas','Americas-SLED',
                'Americas-US Public Sector','Asia Pacific','EMEA','Global Geo','LenovoNetApp']),
        SalesDataOneHotEncoder('AccountSegmentationText', ['0','Commercial','Enterprise 1',
                'Enterprise 1 - Land','Enterprise 2','Enterprise 2 - Land','Global','Not Defined']),
        SalesDataOneHotEncoder('CreatedByUserTypeText', ['Internal','Partner','Zero']),
        SalesDataOneHotEncoder('Renewal_Team_Owned', ['FALSE','TRUE']),
        SalesDataOneHotEncoder('Installed_Base_Type_Text', ['None','Service Renewal','Tech. Refresh','Zero']),
        SalesDataOneHotEncoder('USPS_Federal', ['FALSE','TRUE']),
        SalesDataOneHotEncoder('Channel_Text', ['0','Direct','Indirect']),
        SalesDataOneHotEncoder('End_Customer_Usage_Text', ['Outsourcer','Own Use','Service Provider - Multi Tenant',
                'Service Provider - Single Tenant','Zero'])
    ];
numericalCategories = ['EstimatedBookingUSD_Millions', 'Days_Elapsed_Since_Creation', 'Days_Elapsed_Since_Last_Change'];

salesPhaseOhe = SalesDataOneHotEncoder('SalesPhase', [' 1-Prospecting ',' 2-Qualification ',
        ' 3-Proposal ',' 4-Acceptance ',' 5-Negotiation ',' 6-Closed/Lost ',' 6-Won ']);

modelFileName = "model.json";
weightsFileName = "model_weights.h5";

def readCsvFile(fileName):
    data = pd.read_csv(fileName, dtype={'Renewal_Team_Owned': str, 'USPS_Federal': str});
    return data;

def printDataHead(theData):
    #print(theData.head());
    print(theData.columns);
    print("Shape of the data: {0}".format(theData.shape));

def createANNModel(numInputNodes, numOutputNodes):
    #create model
    model = Sequential();
    #add model layers
    model.add(Dense(30, activation='relu', input_dim = numInputNodes)); # First hidden layer
    #model.add(Dense(10, activation='relu')); # Second hidden layer
    model.add(Dense(numOutputNodes, activation='softmax'));
    model.compile(optimizer='adam', loss='mean_squared_error');
    return model;

def oneHotEncode(fullData, salesDataOhEncoder):
    columnName = salesDataOhEncoder.columnName;
    dataToBeEncoded = fullData[[columnName]];
    ohe = salesDataOhEncoder.ohe;
    categories = salesDataOhEncoder.categories;
    ohe.fit(dataToBeEncoded, categories);
    encodedData = ohe.transform(dataToBeEncoded);
    encArray = encodedData.toarray();
    newData = pd.DataFrame(encArray, columns = ohe.get_feature_names());
    return newData;

def oneHotDecode1Dimension(dataToDecode, salesDataOhEncoder):
	decoded = [salesDataOhEncoder.categoryArray[val] for val in dataToDecode];
	return decoded;

def oneHotDecode(dataToBeDecoded, salesDataOhEncoder):
    columnName = salesDataOhEncoder.columnName;
    ohe = salesDataOhEncoder.ohe;
    decodedData = ohe.inverse_transform(dataToBeDecoded);
    print("decodedData:")
    print(decodedData);
    return decodedData;

def doExit():
    if True:
        exit();

def splitRowsIntoInputAndOutput(data):
    inputVectors = pd.DataFrame();
    for value in oheList:
        oheDf = oneHotEncode(data, value);
        inputVectors = pd.concat([inputVectors, oheDf], axis=1);
    inputVectors = pd.concat([inputVectors, data[numericalCategories]], axis=1);
    printDataHead(inputVectors);
    outputVector = oneHotEncode(data, salesPhaseOhe);
    opportunityIdVector = data[['Opportunity_ID']];
    return inputVectors, outputVector, opportunityIdVector;

def getIndexWithOne(theList):
    index = 0;
    for i in theList:
        if i == 1.0:
            return index;
        index = index + 1;

def loadANNModel():
    #
    # Load the ANN model from file
    #
    print("===========> Reading the model from ", modelFileName);
    savedModelJsonFile = open(modelFileName, "r");
    savedModelJson = savedModelJsonFile.read();
    savedModelJsonFile.close();
    savedModel = model_from_json(savedModelJson);
    savedModel.load_weights(weightsFileName);
    return savedModel;

def trainANN():
    trainDataFile = 'Training Set - 21000 Samples.csv';
    data = readCsvFile(trainDataFile);
    trainDataInputVectors, trainDataOutputVector, trainOpportunityIdVector = splitRowsIntoInputAndOutput(data);
    model = createANNModel(trainDataInputVectors.shape[1], trainDataOutputVector.shape[1]);
    print("===========> Created the ANN model. Now training");
    model.fit(trainDataInputVectors, trainDataOutputVector, epochs=100);
    loss_and_metrics = model.evaluate(trainDataInputVectors, trainDataOutputVector, batch_size=10000);
    print("loss_and_metrics");
    print(loss_and_metrics);
    #plot_model(model, to_file='model.png', show_shapes=True)
    modelJson = model.to_json();
    with open(modelFileName, "w") as jsonFile:
        jsonFile.write(modelJson)
        jsonFile.close();
    model.save_weights(weightsFileName)
    print("===========> Saved the model to ", modelFileName, " and ", weightsFileName);

def testANN():
    #
    # Test the ANN
    #
    print("===========> Reading the model from ", modelFileName);
    #savedModelJsonFile = open(modelFileName, "r");
    #savedModelJson = savedModelJsonFile.read();
    #savedModelJsonFile.close();
    #savedModel = model_from_json(savedModelJson);
    #savedModel.load_weights(weightsFileName);
    savedModel = loadANNModel();
    print("===========> Completed reading the model from ", modelFileName);
    testData = readCsvFile('Validation Set - New.csv');
    print(testData.head());
    testDataInputVectors, testDataOutputVector, testOpportunityIdVector = splitRowsIntoInputAndOutput(testData);
    predictions = savedModel.predict(testDataInputVectors);
    print("===========> Predictions done");
    predictions = np.argmax(predictions, axis=1)
    print('predictions', len(predictions));
    expectedNP = testDataOutputVector.to_numpy();
    expected = [np.where(r==1)[0][0] for r in expectedNP];
    print('expected', len(expected));
    predClasses = oneHotDecode1Dimension(predictions, salesPhaseOhe);
    expClasses = oneHotDecode1Dimension(expected, salesPhaseOhe);
    correctlyPredicted = [i == j for i, j in zip(predictions,expected)];
    resultDf = pd.DataFrame(testOpportunityIdVector);
    resultDf['expected'] = expected;
    resultDf['expClasses'] = expClasses;
    resultDf['predicted'] = predictions;
    resultDf['predClasses'] = predClasses;
    resultDf['correctlyPredicted'] = correctlyPredicted;
    
    rightPredictions = resultDf[resultDf['correctlyPredicted'] == True];
    print("rightPredictions (size: ", rightPredictions.shape, ")");
    print(rightPredictions.head(1000));
    wrongPredictions = resultDf[resultDf['correctlyPredicted'] == False];
    print("wrongPredictions (size: ", wrongPredictions.shape, ")");
    print(wrongPredictions.head(1000));

    cm = confusion_matrix(expected, predictions);
    cmdf = pd.DataFrame(cm, index=salesPhaseOhe.categoryArray, columns=salesPhaseOhe.categoryArray)
    print('Confusion Matrix')
    print(cmdf)
    print('Classification Report')
    targetNames = salesPhaseOhe.categoryArray;
    print(classification_report(expected, predictions, target_names=targetNames))

def main():
    #trainANN();
    testANN();

if __name__== "__main__":
  main()

          

