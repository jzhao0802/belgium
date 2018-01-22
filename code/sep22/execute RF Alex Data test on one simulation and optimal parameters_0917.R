

#using Yan's covariate list


rm(list=ls(all=TRUE))

#initial R programs for random forest model 
library("randomForest")
library("glmnet")
library("doParallel")
library(xlsx)
source("C:\\work\\working materials\\Belgium\\code\\forGermany\\sep21\\BE Project Functions v2 update snowfalls.R")    #!!! please change to your address


#covar_list <- as.vector(read.csv('D:\\jzhao\\Belgium\\Model_Data\\Yan_list.csv', head=T)[, 1])
covar_df <- as.vector(read.csv('C:\\work\\working materials\\Belgium\\data\\preModelData\\Covar_list_v2.csv', head=T)) #465 7
covar_df<- read.xlsx('D:\\jzhao\\Belgium\\Model_Data\\Covar_list_v2.xlsx', sheetName='Covar_list', rowIndex=NULL,
                     startRow=1, endRow=438,
                     as.data.frame=TRUE, header=TRUE, colClasses=NA,
                     keepFormulas=FALSE)
covar_list <- as.vector(covar_df[covar_df[, 2]==1, 1]) #391

simulations <- 4       #changeable
training_portion<- 0.2   #changeable
validation_portion<- 0.2 #changeable

sampling_vector<- c(training_portion, validation_portion)
sampling_vector_forLR <- c(0.3, 0)

model_output_dir<- "C:\\work\\working materials\\Belgium\\Model_output"       #changeable
rawdata_path<- "C:\\work\\working materials\\Belgium\\data\\preModelData"#changeable
rawdata_file<- "statins_pat_v3.csv"                                        #changeable
SeedV<-c(8790, 625699, 561123, 4377881, 1564, 7790325, 98974, 37536615,643, 29117)    #changeable
setwd(model_output_dir)
raw_data <- read.csv(paste(rawdata_path,"/",rawdata_file,sep=""),header=TRUE)
raw_data_yan <- raw_data[, match(covar_list, names(raw_data))] #[1] 65087   393


#check the missing data and delete the corresponding rows
# 1.
missNum_byRow <- apply(apply(raw_data_yan, 1, is.na), 2, sum) 
sum(missNum_byRow > 0 )#52 rows
raw_data_yan_2 <- raw_data_yan[missNum_byRow==0,] #[1] 65035   419
#check the missing data
missNum <- apply(apply(raw_data_yan_2, 1, is.na), 2, sum) 
sum(missNum > 0)
# 2.delete the patients whose pat_gender_0_Max=999 or pat_gender_0_Max+pat_gender_1_Max=2
# and remove the pat_gender_1_Max just keep pat_gender_0_Max
raw_data_yan_2_2 <- raw_data_yan_2[(raw_data_yan_2$pat_gender_0_Max+raw_data_yan_2$pat_gender_1_Max)==1 , -match("pat_gender_1_Max", covar_list)] #61668 418
covar_list <- setdiff(covar_list, 'pat_gender_1_Max')


#check the binary variables with too small positive case
levels <- unlist(lapply(as.vector(covar_list), function(v){
    var <- raw_data_yan_2_2[, v]
    return(length(levels(as.factor(as.character(var)))))
}))
cons_var <- covar_list[levels ==1]
#[1] "atc3_J01X_Max"

binary_list <- covar_list[levels ==2] # are to be transformed as factor 
pos_num <- unlist(lapply(binary_list, function(v){
    pos_num <- sum(raw_data_yan_2_2[,v])
    return(pos_num)
}))
#delete the covariates whose pos case < 10
covar_del_1 <- binary_list[pos_num<10]
covar_del <- c(covar_del_1, cons_var) #33
#create the clean data 1
raw_data_yan_3 <- raw_data_yan_2_2[, match(setdiff(covar_list, covar_del), covar_list)]#[1] [1] 65035   358
#raw_data_yan_3 <- subset(raw_data_yan_2_2, select=-(covar_del))

#transform the binray var into factor
temp=lapply(setdiff(binary_list, covar_del), function(v){
    raw_data_yan_3[, v] <- factor(raw_data_yan_3[, v])
})
for(v in setdiff(binary_list, covar_del)){
    raw_data_yan_3[, v] <- factor(raw_data_yan_3[, v])
    
}

resp_var <- 'persistence_3m'
raw_data_yan_3$response <- raw_data[missNum_byRow == 0 & (raw_data$pat_gender_0_Max+raw_data$pat_gender_1_Max)==1 , resp_var] #[1] 65035   320
#raw_data_yan_3$response <- raw_data[missNum_byRow == 0, resp_var]
important_post_var <- c('market_doc_gender1_Max',
                        'market_doc_gender2_Max',
                        'market_spec11_Max',
                        'market_spec22_Max',
                        'market_spec90_Max')
var_list_all <- names(raw_data)
post_doc_var <- grep('^market_doc|market_spec',var_list_all, value=T) #28
post_mol_doc_var <- grep('^market|^mol_\\w+_Max$', var_list_all, value=T) #33
raw_data_yan_4 <- cbind(raw_data_yan_3, apply(raw_data[missNum_byRow == 0 & (raw_data$pat_gender_0_Max+raw_data$pat_gender_1_Max)==1, post_doc_var], 2, as.factor)) #[1] 65035   387

check_miss_value <- apply(apply(raw_data_yan_4[,], 1, is.na), 2, sum)
sum(check_miss_value>0)

#add patient gender group variable


resp_var <- 'persistence_6m'
raw_data_yan_4$response <- raw_data[missNum_byRow == 0 & (raw_data$pat_gender_0_Max+raw_data$pat_gender_1_Max)==1 , resp_var] #[1] 65035   320

simulation=1
all_samples_forsim<- BE_Sampling(raw_data_yan_4, SeedV[4], sampling_vector)    
s=1

training_data<- all_samples_forsim[[s]][[1]]
validation_data<- all_samples_forsim[[s]][[2]]
test_data <- all_samples_forsim[[s]][[3]]

full_training_data<- rbind(training_data, validation_data)


#add the 5 important post_index covariates

logF <- paste(model_output_dir, "\\Log_RF", "_Yan'slist_test_covar_v2_addPostDoc_gender_spec_addPat_gender_", gsub('(\\w+)_(\\w+)',"\\2", resp_var, perl=T),".csv", sep='')

timeStart <- proc.time()

full_tr_response <- as.factor(full_training_data[,"response"])
full_RF_fit <- randomForest(x=full_training_data[,setdiff(names(full_training_data),c("response"))],y=full_tr_response, 
                            ntree=800,
                            mtry=30,
                            nodesize=10, 
                            importance=T)

imp <- importance(full_RF_fit)
write.csv(imp, paste('covar_importance_covar_v2_addPostDoc_gender_spec_add_patGender', gsub('(\\w+)_(\\w+)',"\\2", resp_var, perl=) ,'.csv', sep=''), row.names=T)

full_tr_pred<- predict(full_RF_fit, full_training_data, type='prob')
full_tr_auc<- auc(full_training_data$response, full_tr_pred[,2])

test_pred <- predict(full_RF_fit, test_data,  type='prob')
test_auc <- auc(test_data$response, test_pred[,2])


#calculate true positive, false positve, true negative and false negative -- using the 0.5 as a threshold

test_perf<-Cal_Perf_Measure(test_pred[,2],0.5,test_data)


saved_sim_outcome<-c(full_tr_auc, test_auc, test_perf) 
timeEnd <- proc.time()
execution_time<- (timeEnd-timeStart)[3]/60
execution_time
write.csv(saved_sim_outcome, paste('model_result_for_covar_v2_addPostDoc_gender_spec_addPat_gender_', gsub('(\\w+)_(\\w+)',"\\2", resp_var, perl=), ".csv", sep=''), row.names=F)
write.csv(test_pred, paste("test_pred_addPostDoc_gender_spec_addPat_gender_", gsub('(\\w+)_(\\w+)',"\\2", resp_var, perl=T),".csv", sep=''))
test_pred_3m <- test_pred
#profiling
#test_data

#add pat_gender_1_Max
n.bucket=3
bucket <- cut(test_pred[, 2], breaks=n.bucket, include.lowest=T,right=F, labels=1:n.bucket)
test_data_1 <- apply(test_data[,], 2, function(i)as.numeric(as.vector(i)))
profile <- t(aggregate(cbind(prediction_value=test_pred[, 2], test_data_1), by=list(bucket), mean))
write.csv(profile, paste('Profile_allVar_withPat_gender_', gsub('(\\w+)_(\\w+)',"\\2", resp_var, perl=T), '_group', n.bucket, '.csv', sep=''), row.names=T)


#add pat_gender_1_Max when profiling the patient
pat_gender <- raw_data$pat_gender_1_Max[missNum_byRow==0]
raw_data_yan_4$pat_gender_1_Max <- as.factor(ifelse(pat_gender==999, NA, pat_gender))
simulation=1
all_samples_forsim_1<- BE_Sampling(raw_data_yan_4, SeedV[simulation], sampling_vector)    
s=1

training_data_1<- all_samples_forsim_1[[s]][[1]]
validation_data_1<- all_samples_forsim_1[[s]][[2]]
test_data_1 <- all_samples_forsim_1[[s]][[3]]

full_training_data<- rbind(training_data_1, validation_data_1)

bucket_3 <- cut(test_pred[, 2], breaks=3, include.lowest=T,right=F, labels=1:3)
bucket_2 <- cut(test_pred[, 2], breaks=2, include.lowest=T,right=F, labels=1:2)
test_data_1 <- apply(test_data_1[,], 2, function(i)as.numeric(as.vector(i)))
profile_1 <- t(aggregate(cbind(prediction_value=test_pred[, 2], test_data_1), by=list(bucket_2), function(x){mean(x, na.rm=T)}))
write.csv(profile_1, 'Profile_allVar_withPat_gender_predictionScore_bucket2.csv', row.names=T)



or <- unlist(lapply(binary_list, function(v){
    tb <- table(full_training_data[, v], full_training_data$response)
    
    covar1_response1 <- tb[2,2]
    covar0_response1 <- tb[1,2]
    covar1_response0 <- tb[2,1]
    covar0_response0 <- tb[1,1]
    contigency_table<- matrix(c(covar1_response1, covar0_response1, covar1_response0, covar0_response0), nc=2, byrow=T)
    association_test<- fisher.test(contigency_table , alternative = "two.sided")
    odds_ratio<- as.vector(association_test$estimate)
    
}))
var_del <- binary_list[is.infinite(or)]

    


#add pat_gender group information
age_group <- read.csv("C:\\work\\working materials\\Belgium\\data\\preModelData\\pat_gender_group.csv",header=F, sep=',')


rate_list <- lapply(as.vector(levels(age_group[, 1])), function(g){
    dt <- age_group[age_group[, 1]==g , ]
    bucket <- cut(dt$V2, breaks=c(-1.1, -0.1, 19, 35, 50, 65, 90), right=T)
    num_sum <- aggregate(dt$V3, by=list(bucket), sum)
    rate_sum <- num_sum$x/sum(dt$V3)
    temp <- cbind(g, num_sum, rate_sum)
    return(temp)
})

library(plyr)
rate_df <- ldply(rate_list, quickdf)
names(rate_df) <- c('Group1', 'Group2', 'Num', 'Rate')
write.csv(rate_df, "C:\\work\\working materials\\Belgium\\data\\preModelData\\pat_gender_group_rate_summary.csv", row.names=F)

decileDt <- read.csv("C:\\work\\working materials\\Belgium\\data\\preModelData\\statins_decile_forPatGenderGroup.csv", header=T)
decileDt$Group <- paste('GR', decileDt[, 3], sep='')
library(dplyr)
head(decileDt)
head(rate_df)

#dt_1 <- semi_join(decileDt, rate_df[, c(1, 4)], by=c('Group'='Group1'))
#dt_2 <- merge(decileDt, rate_df[, c(1, 4)], by.x='Group', by.y='Group1')
resp_var <- 'persistence_6m'
pred <- read.csv(paste("test_pred_addPostDoc_gender_spec_addPat_gender_", gsub('(\\w+)_(\\w+)',"\\2", resp_var, perl=T),".csv", sep=''), header=T)
pred1 <- pred[match(pred[, 1], test_data$response), 3]
library(caret)
res <- as.factor(test_data$response)
pre <- as.factor(ifelse(pred[, 3]>=0.5,'1', '0'))

prd_obj <- cbind(pre, res)
confMatrix <- confusionMatrix(pre, res, positive='1')
confMatrix_df <- cbind(confMatrix$table[, 1], confMatrix$table[, 2])
rownames(confMatrix_df) <- paste('prediction', 0:1, sep='_')
colnames(confMatrix_df) <- paste('Reference', 0:1, sep='_')
write.xlsx(confMatrix_df, 'ConfusionMatrix_RF_sep23.xlsx', sheetName=paste(gsub('(\\w+)_(\\w+)',"\\2", resp_var, perl=T), 'months'), row.names=T, append=T)

