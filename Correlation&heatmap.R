#correlation
medicare=read.csv('medicare_payment.csv')
colnames(medicare) <- c("practitioner","AvgMedPayment")
pharm=read.csv('pharm_payment.csv')
colnames(pharm) <- c("practitioner","AvgPharmPayment")
joindata=merge(x=medicare,y=pharm,by='practitioner',all.x=TRUE)   #join two datafiles
JoinData=na.omit(joindata)         #delete NULL
cor(JoinData$AvgMedPayment,JoinData$AvgPharmPayment)  #correlation=0.038357
ggplot(aes(x=AvgMedPayment,y=AvgPharmPayment),data=JoinData,color=cut)+geom_point()
#write.csv(JoinData,file='combined.csv')        #output joined datafile

#heatmap method1
medicareUSA=read.csv('medicare_USA.csv')
colnames(medicareUSA) <- c("state","AvgMediaPayment")
pharmUSA=read.csv('pharm_USA.csv')
colnames(pharmUSA) <- c("state","AvgPharmPayment")
joindata=merge(x=medicareUSA,y=pharmUSA,by='state',all=TRUE)    #join two datafiles
JoinData=na.omit(joindata)           #delete NULL
#write.csv(JoinData,file='combinedata.csv')

#heatmap using R
row <- JoinData[,1]                            # assign labels in column 1 to "rnames"
mydata <- data.matrix(JoinData[,2:ncol(JoinData)])  # transform column 2-3 into a matrix
rownames(mydata) <- row                  # assign row names
#heatmap(mydata, Rowv=NA, Colv=NA, 
 #     col = heat.colors(256), scale="column", margins=c(5,10))    

my_palette <- colorRampPalette(c("green", "magenta", "darkslategray1"))(n = 299)

# (optional) defines the color breaks manually for a "skewed" color transition
col_breaks = c(seq(0,50,length=100),  # for red
               seq(50,100,length=100),              # for magenta
               seq(100,2000,length=100))              # for darkslategray1

# creates a 5 x 5 inch image
png("heat.png",    # create PNG for the heat map        
    width = 5*400,        # 5 x 400 pixels
    height = 5*400,
    res = 300,            # 300 pixels per inch
    pointsize = 8)        # smaller font size

heatmap.2(mydata, 
          cellnote = mydata,  # same data set for cell labels
          main = "AvgPayment", # heat map title
          notecol="black",      # change font color of cell labels to black
          density.info="none",  # turns off density plot inside color legend
          trace="none",         # turns off trace lines inside the heat map
          margins =c(12,9),     # widens margins around plot
          col=my_palette,       # use on color palette defined earlier 
          #breaks=col_breaks,    # enable color transition at specified limits
          dendrogram="row",     # only draw a row dendrogram
          Colv="NA")            # turn off column clustering

dev.off()               # close the PNG device

#heatmap method2
medUSA=read.csv('py_medUSA.csv',stringsAsFactors=FALSE)
colnames(medUSA) <- c("practitioner","state","AvgMedPayment")
pharmUSA=read.csv('py_PharmUSA.csv',stringsAsFactors=FALSE,)
colnames(pharmUSA) <- c("practitioner","state","AvgPharmPayment")
joindata=merge(x=medUSA,y=pharmUSA,by='practitioner',all=TRUE)  #join two datafiles
JoinData=na.omit(joindata)    #delete nulls
final=JoinData[JoinData[,2]==JoinData[,4],]     #output data only when states match in two datasets 
final[,1]=NULL                          #delete column
final[,4]=NULL
names(final)[1]=paste("state")               #rename 
agg=aggregate(final[c(2,3)],by=list(final$state),FUN=mean)    #calculate averge price based on state
colnames(agg) <- c("state","AvgMediaPayment","AvgPharmPayment")      #rename
write.csv(agg,file='USA.csv')                       #output datafile
