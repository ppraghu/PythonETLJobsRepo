
import csv
import numpy as np
import pandas as pd
import os
import time
from datetime import date, datetime, timedelta
from enum import Enum
from keras.models import Sequential
from keras.layers import Dense
from keras.models import model_from_json
from keras.utils import plot_model
from keras.callbacks import EarlyStopping
from sklearn.neural_network import MLPClassifier
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.model_selection import KFold
from scipy import stats
import seaborn as sb
import matplotlib.pyplot as plt

currentTime = int(time.time());
seed = currentTime;
np.random.seed(seed);

class ModelOutputType(Enum):
    MODEL_WITH_ONE_OUTPUT_NODE = 1
    MODEL_WITH_TWO_OUTPUT_NODES = 2

#MODEL_OUTPUT_TYPE = ModelOutputType.MODEL_WITH_ONE_OUTPUT_NODE;
MODEL_OUTPUT_TYPE = ModelOutputType.MODEL_WITH_TWO_OUTPUT_NODES;

winProbCalculationFromDate = datetime.now();
winProbCalcEffectiveDate = datetime.strptime('01/31/2020 23:59:59', '%m/%d/%Y %H:%M:%S');

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
        SalesDataOneHotEncoder('SalesUnityPartyName', ['Zero','Americas','Americas-SLED',
                'Americas-US Public Sector','Asia Pacific','EMEA','Global Geo','LenovoNetApp']),
        #SalesDataOneHotEncoder('AccountSegmentationText', ['0','Commercial','Enterprise 1',
        #        'Enterprise 1 - Land','Enterprise 2','Enterprise 2 - Land','Global','Not Defined']),
        SalesDataOneHotEncoder('CreatedByUserTypeText', ['Internal','Partner','Zero']),
        SalesDataOneHotEncoder('Renewal_Team_Owned', ['FALSE','TRUE']),
        SalesDataOneHotEncoder('Installed_Base_Type_Text', ['None','Service Renewal','Tech. Refresh','Zero']),
        SalesDataOneHotEncoder('USPS_Federal', ['FALSE','TRUE']),
        SalesDataOneHotEncoder('Channel_Text', ['0','Direct','Indirect']),
        SalesDataOneHotEncoder('End_Customer_Usage_Text', ['Outsourcer','Own Use','Service Provider - Multi Tenant',
                'Service Provider - Single Tenant','Zero'])
    ];
numericalCategories = ['EstimatedBookingUSD_Millions'];

salesPhaseWiseProbabilities = {
    ' 1-Prospecting ': 0.01, # Close to 0% probability - Make it 1%
    ' 2-Qualification ': 0.20, # 20% probability
    ' 3-Proposal ': 0.5, # 50% probability
    ' 4-Acceptance ': 0.75, # 75% probability
    ' 5-Negotiation ': 1.0, # 100% probability
    ' 7-Closed/Lost ': 0.0, # 0% probability
    ' 6-Won ': 1.0};
        
#
# Take only Win and Lost opportunities
#
salesPhaseOhe = SalesDataOneHotEncoder('SalesPhase', [' 7-Closed/Lost ',' 6-Won ']);

rawModelFileName = "model_{0}_OutputNeurons.json";
rawWeightsFileName = "model_weights_{0}_OutputNeurons.h5";
rawModelImageFileName = "model_diagram_{0}_OutputNeurons.png"

def getNeuralNetModelFileNames():
    modelFileName = rawModelFileName.format(MODEL_OUTPUT_TYPE.value);
    weightsFileName = rawWeightsFileName.format(MODEL_OUTPUT_TYPE.value);
    modelImageFileName = rawModelImageFileName.format(MODEL_OUTPUT_TYPE.value);
    return modelFileName, weightsFileName, modelImageFileName;
    
def deleteFile(filename):
    if os.path.exists(filename):
        os.remove(filename)

def printDataHead(msg, theData):
    print("===================================================");
    print("Printing the data for " + msg);
    print(theData.head(10));
    print(theData.columns);
    print("Shape of the data: {0}".format(theData.shape));
    print("===================================================");

def readCsvFile(fileName):
    data = pd.read_csv(fileName, dtype={'Renewal_Team_Owned': str, 'USPS_Federal': str});
    return data;

def getOnlyWonLostOpportunities(origData):
    data = origData.copy();
    data = data.loc[(data['SalesPhase'] == ' 7-Closed/Lost ') | (data['SalesPhase'] == ' 6-Won ')]
    data.reset_index(drop=True, inplace=True);
    wonCount = len(data[data['SalesPhase'] == ' 6-Won ']);
    lostCount = len(data[data['SalesPhase'] == ' 7-Closed/Lost ']);
    totalCount = len(data);
    print("wonCount[", wonCount, "], lostCount[", lostCount, "], totalCount[", totalCount, "]");
    return data;

def getOnlyWonOpportunities(origData):
    data = origData.copy();
    data = data.loc[(data['SalesPhase'] == ' 6-Won ')];
    data.reset_index(drop=True, inplace=True);
    wonCount = len(data[data['SalesPhase'] == ' 6-Won ']);
    print("wonCount[", wonCount, "]");
    return data;

def getOnlyLostOpportunities(origData):
    data = origData.copy();
    data = data.loc[(data['SalesPhase'] == ' 7-Closed/Lost ')];
    data.reset_index(drop=True, inplace=True);
    lostCount = len(data[data['SalesPhase'] == ' 7-Closed/Lost ']);
    print("lostCount[", lostCount, "]");
    return data;

def getDataExcludingWonLostOpportunities(origData):
    data = origData.copy();
    data = data.loc[~((data['SalesPhase'] == ' 7-Closed/Lost ') | (data['SalesPhase'] == ' 6-Won '))]
    data.reset_index(drop=True, inplace=True);
    return data;

def createANNModel(numInputNodes, numOutputNodes):
    print("===========> Creating the ANN model with", numInputNodes, " input nodes and", numOutputNodes, " output nodes");
    #create model
    model = Sequential();
    #add model layers
    model.add(Dense(70, activation='relu', input_dim = numInputNodes)); # First hidden layer
    model.add(Dense(35, activation='relu')); # Second hidden layer
    model.add(Dense(15, activation='relu')); # Third hidden layer
    if MODEL_OUTPUT_TYPE == ModelOutputType.MODEL_WITH_ONE_OUTPUT_NODE:
        model.add(Dense(numOutputNodes, activation='sigmoid'));
        model.compile(optimizer='adam', loss='mean_squared_error');
    else:
        model.add(Dense(numOutputNodes, activation='softmax'));
        model.compile(optimizer='adam', loss='categorical_crossentropy');
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
    return decodedData;

def doExit():
    if True:
        exit();

def splitRowsIntoInputAndOutput(data):
    inputVectors = pd.DataFrame();
    for value in oheList:
        print("OneHotEncode for ", value.columnName);
        oheDf = oneHotEncode(data, value);
        inputVectors = pd.concat([inputVectors, oheDf], axis=1);
    numericalCategoriesData = data[numericalCategories];
    inputVectors = pd.concat([inputVectors, numericalCategoriesData], axis=1);
    oneHotEncodedWinLossSalesPhases = oneHotEncode(data, salesPhaseOhe);
    opportunityIdVector = data[['Opportunity_ID']];
    wonOrLostProbability = pd.DataFrame();
    wonOrLostProbability['WonProbability'] = np.where(data['SalesPhase']==' 6-Won ', 1, 0)
    if MODEL_OUTPUT_TYPE == ModelOutputType.MODEL_WITH_ONE_OUTPUT_NODE:
        return inputVectors, wonOrLostProbability, opportunityIdVector;
    else:
        return inputVectors, oneHotEncodedWinLossSalesPhases, opportunityIdVector;

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
    modelFileName, weightsFileName, modelImageFileName = getNeuralNetModelFileNames();
    print("===========> Reading the model from ", modelFileName);
    savedModelJsonFile = open(modelFileName, "r");
    savedModelJson = savedModelJsonFile.read();
    savedModelJsonFile.close();
    savedModel = model_from_json(savedModelJson);
    savedModel.load_weights(weightsFileName);
    print("===========> Completed reading the model from ", modelFileName);
    return savedModel;

def saveANNModel(model):
    modelFileName, weightsFileName, modelImageFileName = getNeuralNetModelFileNames();
    plot_model(model, to_file=modelImageFileName, show_shapes=True)
    modelJson = model.to_json();
    deleteFile(modelFileName);
    deleteFile(weightsFileName);
    with open(modelFileName, "w") as jsonFile:
        jsonFile.write(modelJson)
        jsonFile.close();
    model.save_weights(weightsFileName)
    print("===========> Saved the model to ", modelFileName, " and ", weightsFileName);

def trainANN(fullData):
    data = getOnlyWonLostOpportunities(fullData);
    trainDataInputVectors, trainDataOutputVector, trainOpportunityIdVector = splitRowsIntoInputAndOutput(data);
    model = createANNModel(trainDataInputVectors.shape[1], trainDataOutputVector.shape[1]);
    print("===========> Created the ANN model. Now training");
    
    kfold = KFold(n_splits=20, shuffle=True, random_state=seed);
    kfold.get_n_splits(trainDataInputVectors, trainDataOutputVector);
    cvscores = [];
    fold = 1;
    for train_index, test_index in kfold.split(trainDataInputVectors, trainDataOutputVector):
        print("Fold ", fold);
        print("TRAIN size:", len(train_index)); 
        print("TEST size:", len(test_index));
        X_train, X_test = trainDataInputVectors.loc[train_index], trainDataInputVectors.loc[test_index]
        y_train, y_test = trainDataOutputVector.loc[train_index], trainDataOutputVector.loc[test_index]
        # Fit the model
        model.fit(X_train, y_train, epochs=100, batch_size=10000)
        # evaluate the model
        scores = model.evaluate(X_test, y_test, verbose=0)
        print("evaluate output - ", model.metrics_names, scores)
        #cvscores.append(scores[1] * 100)
        #print("%.2f%% (+/- %.2f%%)" % (numpy.mean(cvscores), numpy.std(cvscores)))
        fold = fold + 1;
    
    #model.fit(trainDataInputVectors, trainDataOutputVector, epochs=5);
    #loss_and_metrics = model.evaluate(trainDataInputVectors, trainDataOutputVector, batch_size=10000);
    #print("loss_and_metrics");
    #print(loss_and_metrics);
    saveANNModel(model);

def testANN(fullData):
    #
    # Test the ANN
    #
    savedModel = loadANNModel();
    testData = getOnlyWonLostOpportunities(fullData);
    testDataInputVectors, testDataOutputVector, testOpportunityIdVector = splitRowsIntoInputAndOutput(testData);
    predictionsRaw = savedModel.predict(testDataInputVectors);
    currentSalesPhases = testData[['SalesPhase']];
    resultDf = pd.DataFrame(testOpportunityIdVector);
    resultDf['currentSalesPhase'] = currentSalesPhases;
    if MODEL_OUTPUT_TYPE == ModelOutputType.MODEL_WITH_ONE_OUTPUT_NODE:
        winProbability = predictionsRaw[:,0];
        lossProbability = (1 - winProbability);
    else:
        winProbability = predictionsRaw[:,1];
        lossProbability = predictionsRaw[:,0];

    resultDf['WinProbability'] = winProbability.round(6);
    resultDf['LossProbability'] = lossProbability.round(6);
    predictedToWin = [i > 0.5 for i in winProbability];
    resultDf['predictedToWin'] = predictedToWin;
    predictions = [1 if i == True else 0 for i in predictedToWin];
    expected = [1 if i == ' 6-Won ' else 0 for i in currentSalesPhases['SalesPhase']];
    predClasses = oneHotDecode1Dimension(predictions, salesPhaseOhe);
    correctlyPredicted = [i == j for i, j in zip(predClasses,currentSalesPhases['SalesPhase'].tolist())];
    resultDf['predicted'] = predictions;
    resultDf['predClasses'] = predClasses;
    resultDf['correctlyPredicted'] = correctlyPredicted;
    printDataHead("testANN - resultDf", resultDf);

    rightPredictions = resultDf[resultDf['correctlyPredicted'] == True];
    wrongPredictions = resultDf[resultDf['correctlyPredicted'] == False];
    printDataHead("rightPredictions", rightPredictions);
    printDataHead("wrongPredictions", wrongPredictions);

    cm = confusion_matrix(expected, predictions);
    cmdf = pd.DataFrame(cm, index=salesPhaseOhe.categoryArray, columns=salesPhaseOhe.categoryArray)
    print('Confusion Matrix')
    print(cmdf)
    print('Classification Report')
    targetNames = salesPhaseOhe.categoryArray;
    print(classification_report(expected, predictions, target_names=targetNames))

#
# Test with open opportunities
#
def computeWinProbabilityFromANN(openOpportunities):
    savedModel = loadANNModel();
    printDataHead("Full Validation Set.csv", openOpportunities);
    currentSalesPhases = openOpportunities[['SalesPhase']];
    testDataInputVectors, testDataOutputVector, testOpportunityIdVector = splitRowsIntoInputAndOutput(openOpportunities);
    predictionsRaw = savedModel.predict(testDataInputVectors);
    resultDf = pd.DataFrame(testOpportunityIdVector);
    resultDf = pd.concat([resultDf, openOpportunities[['EstimatedBookingUSD_Millions']]], axis=1);
    resultDf['currentSalesPhase'] = currentSalesPhases;
    if MODEL_OUTPUT_TYPE == ModelOutputType.MODEL_WITH_ONE_OUTPUT_NODE:
        winProbability = predictionsRaw[:,0];
        lossProbability = (1 - winProbability);
    else:
        winProbability = predictionsRaw[:,1];
        lossProbability = predictionsRaw[:,0];
    resultDf['WinProbabilityFromANN'] = winProbability.round(6);
    resultDf['LossProbabilityFromANN'] = lossProbability.round(6);
    predictedToWin = [i > 0.5 for i in winProbability];
    resultDf['predictedToWin'] = predictedToWin;

    printDataHead("applyANNToOpenOpportunities - resultDf", resultDf);
    wonPredictions = resultDf[resultDf['predictedToWin'] == True];
    lostPredictions = resultDf[resultDf['predictedToWin'] == False];
    printDataHead("wonPredictions", wonPredictions);
    printDataHead("lostPredictions", lostPredictions);
    return resultDf;

def simpleCDF(mode, age, a, loc, scale):
    return stats.skewnorm.cdf(age, a, loc, scale);

def complexCDF(mode, cdfUntilMode, age, a, loc, scale):
    cdfTillAgeOfOpportunity = stats.skewnorm.cdf(age, a, loc, scale);
    effectiveCDF = (2 * cdfUntilMode) - cdfTillAgeOfOpportunity;
    #print("mode, cdfUntilMode, age, cdfTillAgeOfOpportunity, effectiveCDF:", mode, cdfUntilMode, age, cdfTillAgeOfOpportunity, effectiveCDF);
    return effectiveCDF;

def computeCDF(mode, cdfUntilMode, age, a, loc, scale):
    effectiveCDF = 0;
    if age <= mode:
        effectiveCDF = stats.skewnorm.cdf(age, a, loc, scale);
    else:
        cdfTillAgeOfOpportunity = stats.skewnorm.cdf(age, a, loc, scale);
        effectiveCDF = (2 * cdfUntilMode) - cdfTillAgeOfOpportunity;
        #print("mode, cdfUntilMode, age, cdfTillAgeOfOpportunity, effectiveCDF:", mode, cdfUntilMode, age, cdfTillAgeOfOpportunity, effectiveCDF);
    return effectiveCDF;

def computeSkewNormDistributionParameters(fullData):
    wonData = getOnlyWonOpportunities(fullData);
    ageData = wonData[['Days_Elapsed_Since_Creation']];
    printDataHead("ageData", ageData);
    sb.distplot(ageData, color='r');
    a, loc, scale = stats.skewnorm.fit(ageData.to_numpy());
    print("Skew norm params", a, loc, scale);
    mean = stats.skewnorm.mean(a, loc, scale);
    median = stats.skewnorm.median(a, loc, scale);
    mode = stats.mode(ageData)[0][0][0];
    print("Skew norm mean, median, mode", mean, median, mode);
    dataNumRows = len(ageData);
    skewNormDistribution = stats.skewnorm(a, loc, scale);
    sample = skewNormDistribution.rvs(dataNumRows)
    sb.distplot(sample, color='b');
    plt.show();
    return {'a': a, 'loc': loc, 'scale': scale, 'mode': mode};

def computeAgeRelatedCumulativeWinProbability(openOpportunities, skewNormParameters, resultDf):
    #wonData = getOnlyWonOpportunities(fullData);
    #ageData = wonData[['Days_Elapsed_Since_Creation']];
    #printDataHead("ageData", ageData);
    #sb.distplot(ageData, color='r');
    #a, loc, scale = stats.skewnorm.fit(ageData.to_numpy());
    #print("Skew norm params", a, loc, scale);
    #mean = stats.skewnorm.mean(a, loc, scale);
    #median = stats.skewnorm.median(a, loc, scale);
    #mode = stats.mode(ageData)[0][0][0];
    #print("Skew norm mean, median, mode", mean, median, mode);
    #dataNumRows = len(ageData);
    #skewNormDistribution = stats.skewnorm(a, loc, scale);
    #sample = skewNormDistribution.rvs(dataNumRows)
    #sb.distplot(sample, color='b');
    #plt.show();
    #
    # Temp logic to get createdDate
    #
    #openOpportunities = getDataExcludingWonLostOpportunities(fullData);
    dateData = pd.DataFrame(index=openOpportunities.index);
    dateData['Today'] = pd.Timestamp.now();
    td = pd.DataFrame([pd.Timedelta(days=i) for i in openOpportunities['Days_Elapsed_Since_Creation']]);
    dateData['TD'] = td;
    printDataHead("dateData", dateData);
    dateData['createdDate'] = dateData['Today'] - dateData['TD'];
    printDataHead("dateData with createdDate", dateData);
    openOpportunities['createdDate'] = dateData['createdDate'];
    printDataHead("openOpportunities with createdDate", openOpportunities);
    #
    #
    
    openOpportunities['WinProbCalcEffectiveDate'] = winProbCalcEffectiveDate;
    ageAsOfProbCalculationToDate = (openOpportunities['WinProbCalcEffectiveDate'] - openOpportunities['createdDate']).astype('timedelta64[D]');

    #
    # Logic of CDF calculation:
    # If age of an opportunity is less than the mode of the WonAge ditribution, get CDF for the age
    # If age of an opportunity is more than the mode of the WonAge distribution, effective CDF =
    #       2 * CDF(age = mode) - CDF(age = actual age)
    #
    a = skewNormParameters['a'];
    loc = skewNormParameters['loc'];
    scale = skewNormParameters['scale'];
    mode = skewNormParameters['mode'];
    cdfUntilMode = stats.skewnorm.cdf(mode, a, loc, scale);
    #cdf = [simpleCDF(mode, i, a, loc, scale) if i <= mode else complexCDF(mode, cdfUntilMode, i, a, loc, scale) for i in ageAsOfProbCalculationToDate];
    cdf = [computeCDF(mode, cdfUntilMode, i, a, loc, scale) for i in ageAsOfProbCalculationToDate];
    resultDf["createdDate"] = dateData['createdDate'];
    resultDf["AgeAsOfProbCalculationToDate"] = ageAsOfProbCalculationToDate;
    resultDf["AgeRelatedCumulativeWinProbability"] = cdf;
    return resultDf;

def computeProbabilityBasedOnCurrentSalesPhase(openOpportunities, resultDf):
    currentSalesPhases = openOpportunities[['SalesPhase']];
    phaseWiseProbabilities = currentSalesPhases['SalesPhase'].map(salesPhaseWiseProbabilities);
    resultDf['PhaseWiseProbability'] = phaseWiseProbabilities;
    return resultDf;

def main():
    #
    # Train the ANN and extract statistical parameters
    #
    #fullData = readCsvFile('Full Validation Set.csv');
    fullData = readCsvFile('SalesPipelineDataExtract - Jan-Dec 2019 - RequiredColumnsOnly.csv');
    #trainANN(fullData);
    #testANN(fullData);
    skewNormParameters = computeSkewNormDistributionParameters(readCsvFile('Full Validation Set.csv'));
    #
    # Apply the model to Open opportunities
    #
    openOpportunities = getDataExcludingWonLostOpportunities(fullData);
    resultDf = computeWinProbabilityFromANN(openOpportunities);
    resultDf = computeAgeRelatedCumulativeWinProbability(openOpportunities, skewNormParameters, resultDf);
    resultDf = computeProbabilityBasedOnCurrentSalesPhase(openOpportunities, resultDf);
    resultDf["EffectiveProbability"] = resultDf['WinProbabilityFromANN'] * resultDf["AgeRelatedCumulativeWinProbability"] * resultDf['PhaseWiseProbability'];
    resultDf["WeightedPipelineDollarsinMillion"] = (resultDf["EffectiveProbability"] * resultDf["EstimatedBookingUSD_Millions"]).round(10);
    printDataHead("main() - resultDf\n", resultDf);
    resultDf.to_csv("FinalOutput.csv");


if __name__== "__main__":
    main()

          

