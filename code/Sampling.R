sampling <- function(data, prop.tr, prop.val, prop.test) {
    if (!(prop.tr<=1 & prop.tr >=0 & prop.val<=1 & prop.val>=0 & prop.test<=1 & prop.test>=0 & sum(prop.tr+prop.val+prop.test)==1)) stop("Please confirm the proportion!!")
    pos_idx <- which(data$response==1)
    neg_idx <- which(data$response==0)
    get_idx <- function(label_idx, prop.tr, prop.val, prop.test){
         
        tr_idx <- sample(label_idx, prop.tr*length(label_idx), replace=FALSE)
        val_idx <- sample(setdiff(label_idx, tr_idx), prop.val*length(label_idx), replace=FALSE)
        test_idx <- setdiff(label_idx, c(tr_idx, val_idx))
        return(list(tr_idx=tr_idx, val_idx=val_idx, test_idx=test_idx))
    }
    pos_idx_list <- get_idx(label_idx=pos_idx, prop.tr, prop.val, prop.test)
    neg_idx_list <- get_idx(label_idx=neg_idx, prop.tr, prop.val, prop.test)
    
    flag <- numeric(nrow(data))
    flag[c(pos_idx_list[[1]], neg_idx_list[[1]])]=1
    flag[c(pos_idx_list[[2]], neg_idx_list[[2]])]=2
 
    return(flag)
}