For training the model
For the parameters in this list ['PT08.S1(CO)', 'C6H6(GT)', 'PT08.S2(NMHC)', 'PT08.S3(NOx)', 'PT08.S4(NO2)', 'PT08.S5(O3)', 'T', 'RH', 'AH'], I replace the missing value with representative value, mean since there are many missing value. I think if I drop out the whole record that contains missing value in this list of parameters, I might miss a significant number of data I can use to train the model.
For these 3 parameters, ['CO(GT)', 'NOx(GT)', 'NO2(GT)'], there are not many missing values, so I decide to drop the record that one of these three variables is missing.
For NMHC(GT), since there are a lot of missing value, I think the remaining data is not enough to train the model, so I decide to ignore this parameter.

For predicting values
If the record has missing value in one of these parameters ['PT08.S1(CO)', 'C6H6(GT)', 'PT08.S2(NMHC)', 'PT08.S3(NOx)', 'PT08.S4(NO2)', 'PT08.S5(O3)', 'T', 'RH', 'AH', 'CO(GT)', 'NOx(GT)', 'NO2(GT)'], I will replace the missing value with mean of that variable.
For missing value of NMHC(GT), since our model does not take this into account, I will ignore it and predict the result based on other parameters.