#Belguim predict modeling project
#checking correlation between Icomed data and persistent days/persistence

#load required library
library(xlsx)
library(XLConnect)
library(plyr)
library(dplyr)

#define the path
basePath <- 'C:\\work\\working materials\\Belgium\\'
inPath <- paste(basePath, 'data\\toBeMergedData\\', sep='')
outPath <- 'C:\\work\\working materials\\Belgium\\data\\preModelData\\Oct09\\'
setwd(outPath)
#read in income data and raw_data
#read.xlsx(file, sheetIndex, sheetName=NULL, rowIndex=NULL,
#startRow=NULL, endRow=NULL, colIndex=NULL,
#as.data.frame=TRUE, header=TRUE, colClasses=NA,
#keepFormulas=FALSE, encoding="unknown", ...)

IcomeDt1 <- read.xlsx(paste(inPath, 'Icomed data at physician level.xlsx', sep='')
                      , sheetName='data', startRow=1, endRow=63388
                      , as.data.frame=TRUE, header=TRUE)
IcomeDt <- readWorksheetFromFile(file=paste(inPath, 'Icomed data at physician level.xlsx', sep='')
                                 , sheet='data', region="A1: BL63388", header=T)
dim(IcomeDt) #[1] 63387    64
names(IcomeDt) <- tolower(names(IcomeDt))
check_miss <- sum(apply(IcomeDt, 1, function(x)all(!is.na(x))))

statinsDt <- read.csv(paste(basePath, 'statins.csv', sep=''), header=T)
names(statinsDt) <- tolower(names(statinsDt))

statinsDt$shortcode <- as.character(statinsDt$shortcode)

merged1 <- left_join(statinsDt, IcomeDt, by=c('shortcode'='short_id'))

rm(list=c('statinsDt', 'IcomeDt'))
#a. get match rate
matchRate <- sum(!is.na(merged1$shortcode))/nrow(merged1)
#b. calculate the record number with any of the 3 variable non-missing -- C001, C003, C004.
num_rcd <- sum(apply(merged1[, match(c('c001', 'c003', 'c004'), names(merged1))], 1, function(x)!all(is.na(x))))
#[1] 1511847
#Calculate number of records retained and what percentage of records that have non-missing physician survey info
num_rcd_nonMiss <- sum(apply(merged1, 1, function(x)all(!is.na(x))))

#the match rate
matched_phy <- statinsDt$shortcode[which(!is.na(match(statinsDt$shortcode, IcomeDt$short_id)))]
matched_phy2 <- IcomeDt$short_id[which(!is.na(match( IcomeDt$short_id, statinsDt$shortcode)))]
#[1] 37029
matched_phy2_full <- IcomeDt[match(matched_phy2, IcomeDt$short_id),]
valid_2 <- matched_phy2_full[apply(matched_phy2_full[, match(c('c001', 'c003', 'c004'), names(matched_phy2_full))], 1, function(x)!all(is.na(x))) ,]
#[1] 3499   64

sum(!is.na(match(statinsDt$shortcode, IcomeDt$short_id)))
#check the non-missing rate for IcomeDt
IcomeDt1 <- IcomeDt[apply(IcomeDt[, match(c('c001', 'c003', 'c004'), names(IcomeDt))], 1, function(x)!all(is.na(x))) ,]
#[1] 4133   64
nonMiss_rate <- apply(apply(IcomeDt1, 2, function(x){!is.na(x) & x != ""}), 2, function(x)sum(x)/length(x))
write.csv(nonMiss_rate, 'NonMiss_rate_for_validIcome.csv', row.names=T)

merge_valid <- read.csv( "merge_valid.csv", header=T)
names(merge_valid) <- tolower(names(merge_valid))
var1 <- grep('^c\\d+', names(merge_valid), value=T, perl=T)

matched_phyId <- as.character(unique(merge_valid$shortcode))
IcomeDt1_matched <- IcomeDt1[match(matched_phyId, IcomeDt1$short_id), ]
#[1] 3499   64
nonMiss_rate_matched <- apply(apply(IcomeDt1_matched, 2, function(x){!is.na(x) & x != ""}), 2, function(x)sum(x)/length(x))
write.csv(nonMiss_rate_matched, 'NonMiss_rate_for_validIcome_matched.csv', row.names=T)



#the following method is not right
var1_fct_flag <- unlist(lapply(var1, function(v){
    return(is.factor(merge_valid[, v]))
}))
var1_fct <- var1[var1_fct_flag]
to_Character <- apply(merge_valid[, match(var1, names(merge_valid))], 2, function(x){
    if(!is.integer(x)){
        return(as.character(x))
    }else{
        return(x)
    }
})
nonMiss_rate_valid_matched <- apply(apply(to_Character, 2, function(x){!is.na(x) & x!=""}), 2, function(x)sum(x)/length(x))
write.csv(nonMiss_rate_valid_matched, 'NonMiss_rate_for_validIcome_matched.csv', row.names=T)
rm(list=c('to_Character'))
#end of 



#check the complete physician for the target IcomeCode
#read in target IcomeCode list
tarCodeList <- as.character(read.csv('target_IcomeCode.csv', head=FALSE)[, 1])
IcomeDt2 <- IcomeDt1_matched[, tarCodeList]#[1] 3499   17
lapply(tarCodeList, function(v){
    vct <- IcomeDt2[, v]
    return(table(vct))
})

num_complete <- sum(apply(IcomeDt2, 1, function(x){all(!is.na(x))}))#[1] 756
complete_rate <- num_complete/nrow(IcomeDt2)#[1] 0.2160617

#1. ????????????????????????3499???????????????????????????; 2. ????????????????????????756??????????????????????????????

pat_1 <- unique(statinsDt[!is.na(match(statinsDt$shortcode, valid_2$short_id)), 'p_id'])
length(pat_1) #[1] 32138
length(unique(statinsDt$p_id)) #[1] 67911

pat_2 <- unique(statinsDt[!is.na(match(statinsDt$shortcode, valid_2$short_id[apply(IcomeDt2, 1, function(x){all(!is.na(x))})])), 'p_id'])
length(pat_2) #[1] 8966

doc_spec_1 <- unique(statinsDt[!is.na(match(statinsDt$shortcode, valid_2$short_id)) , 'doc_speciality'])
doc_spec_2 <- unique(statinsDt[!is.na(match(statinsDt$shortcode, valid_2$short_id[apply(IcomeDt2, 1, function(x){all(!is.na(x))})])) , 'doc_speciality'])

write.xlsx(doc_spec_1, 'Unique_doc_specialty.xlsx', sheetName='3499', append=TRUE)
write.xlsx(doc_spec_2, 'Unique_doc_specialty.xlsx', sheetName='756', append=TRUE)
write.xlsx(doc_spec, 'Unique_doc_specialty.xlsx', sheetName='67911', append=TRUE)

#correlation
tarCodeList1 <- grep('c0[6,7]\\d', names(IcomeDt2), value=T, perl=T)
IcomeDt3 <- IcomeDt1_matched[, c('short_id', tarCodeList1)]#[1] 3499   11

IcomeDt3_comp <- IcomeDt3[apply(IcomeDt3, 1, function(x){all(!is.na(x))}), ]#[1] 1506   11

phyId <- unique(IcomeDt3_comp$short_id)

merge_valid_1 <- merge_valid[!is.na(match(merge_valid$shortcode, phyId)), c('p_id', 'capped_days_treatment', 'shortcode', 'market', tarCodeList1)]#[1] 772849     13
market <- merge_valid[!is.na(match(merge_valid$shortcode, phyId)), 'market']
merge_valid_1_preIndex <- merge_valid_1[market==1 , ] #[1] 45822    13
#QC
length(unique(as.character(merge_valid_1$shortcode)))#1506
length(unique(as.character(merge_valid_1$p_id)))#17300
length(unique(as.character(merge_valid_1_preIndex$shortcode)))#1356
length(unique(as.character(merge_valid_1_preIndex$p_id)))#10588


forCorDt <- aggregate(merge_valid_1_preIndex[, -match(c('p_id', 'shortcode'), names(merge_valid_1_preIndex))], by=list(merge_valid_1_preIndex$p_id), mean) #[1] 17300    12
names(forCorDt) <- gsub('^Group\\W\\w+', 'P_id', names(forCorDt))
#QC missing
miss_check <- apply(apply(forCorDt, 2, is.na), 2, sum)
miss_var_num <- sum(miss_check > 0)

pers <- forCorDt$capped_days_treatment
pers_1 <- ifelse(pers/30 >= 6, 1, 0)
corr <- unlist(lapply(tarCodeList1, function(v){
    cor <- cor(pers, forCorDt[, v], method='pearson')
    return(cor)
}))
corr_1 <- unlist(lapply(tarCodeList1, function(v){
    cor <- cor(pers_1, forCorDt[, v], method='spearman')
    return(cor)
}))

corr_out <- cbind(corr_1=corr, corr_2=corr_1)
rownames(corr_out) <- tarCodeList1
colnames(corr_out) <- paste('Correlation with ', c('persistent days', 'persistent_6m'), sep='')
corr_out_order <- corr_out[order(abs(corr_out[, 1]), decreasing=T),]
write.xlsx(corr_out_order, 'Corr_result.xlsx', sheetName='Pre and on Index', append=T, row.names=T)
