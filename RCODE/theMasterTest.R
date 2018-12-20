
if(getwd() == "C:/WINDOWS/system32") setwd("C:/Users/dgteam/Desktop/Balancing Production/production-balancing")


if( as.numeric( substr(Sys.time(),12,13) ) %in% c(7:17) ) {
  
  
  library(sqldf)
  library(DBI)
  library(odbc)
  library(xlsx)
  library(openxlsx)
  library(RSAP)
  library(reshape2)
  library(lubridate)
  
  
  
  errorcount = 0
  errormessage = NA
  
  
  source("RCODE/theFunctionsTest.R")
  
  
  g( worked , KOresult , BWresult , HANAresult ) %=% testConnection()
  
  mmworked = runMM()
  
  
  if(worked == TRUE) {
    
    WhatBroke = c("Senior" , "BW" , "HANA")[c(KOresult , BWresult , HANAresult) == "Error"]
    WhatBroke = paste(WhatBroke,collapse = " ")
    
    print(paste("These things broke" , WhatBroke))
    
  }
  
  
  
  if(worked == FALSE) {
    
    
    print("Pulling Last Period Data")
    g(PEPERIOD,PEYEAR,PEtheDATE) %=% QDATE(F)
    
    
    print("Pulling Last Period BW")
    PEBWVolume = BW(PEYEAR,PEPERIOD)
    
    print("Pulling Last Period KO")
    PEKOEXTT = KONEW(PEYEAR,PEPERIOD,CURRENTPERIOD = F)
    
    print("Pulling Last Period HANA")
    PEHANA = COPA(PEYEAR,PEPERIOD)
    
    print("Pulling Last Period MM")
    g(PEGLMM,PEPhyMM) %=% MM(PE = T)
    
    print("Pulling Last Period ZRTL")
    PEZR = ZRTLRS(PEPERIOD)
    
    g(PEVolumePhysical,PEVolumeGL,PENSI,PECOGS) %=% mergeall(PEHANA,PEKOEXTT,PEBWVolume,PEGLMM,PEPhyMM,PEZR,PEYEAR,PEPERIOD,PEtheDATE,"YES")
    
    
    
    
    
    print("Pulling Current Period Data")
    g(PERIOD,YEAR,theDATE) %=% QDATE(T)
    
    print("Pulling Current Period BW")
    BWVolume = BW(YEAR,PERIOD)
    
    print("Pulling Current Period KO")
    KOEXTT = KONEW(YEAR,PERIOD,CURRENTPERIOD = T)
    
    # HANA = COPA(YEAR,PERIOD)
    print("Pulling Current Period HANA")
    HANA = COPACurrent(KOEXTT)
    
    
    print("Pulling Current Period MM") 
    g(GLMM,PhyMM) %=% MM(PE = F)
    
    print("Pulling Current Period ZRTL")
    ZR = ZRTLRS(PERIOD)
    
    g(VolumePhysical,VolumeGL,NSI,COGS) %=% mergeall(HANA,KOEXTT,BWVolume,GLMM,PhyMM,ZR,YEAR,PERIOD,theDATE,"NO")
    
    
    
    
    # Write to SQL
    

    conKO <- dbConnect(odbc(), "senior",uid = usr , pwd = pass)
    
    
    theTime = as.character( Sys.time() )
    
    print( dim(dbGetQuery(conKO , "SELECT * FROM SWIRE.BALANCINGPHYSICAL" ) ) )
    writetoPhysical(PEVolumePhysical,theTime)
    print( dim(dbGetQuery(conKO , "SELECT * FROM SWIRE.BALANCINGPHYSICAL" ) ) )
    writetoPhysical(VolumePhysical,theTime)
    print( dim(dbGetQuery(conKO , "SELECT * FROM SWIRE.BALANCINGPHYSICAL" ) ) )
    
    
    print( dim(dbGetQuery(conKO , "SELECT * FROM SWIRE.BALANCINGGL" ) ) )
    writetoGL(PEVolumeGL,theTime)
    print( dim(dbGetQuery(conKO , "SELECT * FROM SWIRE.BALANCINGGL" ) ) )
    writetoGL(VolumeGL,theTime)
    print( dim(dbGetQuery(conKO , "SELECT * FROM SWIRE.BALANCINGGL" ) ) )
    
    
    
    print( dim(dbGetQuery(conKO , "SELECT * FROM SWIRE.BALANCINGNSI" ) ) )
    writetoNSI(PENSI,theTime)
    print( dim(dbGetQuery(conKO , "SELECT * FROM SWIRE.BALANCINGNSI" ) ) )
    writetoNSI(NSI,theTime)
    print( dim(dbGetQuery(conKO , "SELECT * FROM SWIRE.BALANCINGNSI" ) ) )
    
    
    
    print( dim(dbGetQuery(conKO , "SELECT * FROM SWIRE.BALANCINGCOGS" ) ) )
    writetoCOGS(PECOGS,theTime)
    print( dim(dbGetQuery(conKO , "SELECT * FROM SWIRE.BALANCINGCOGS" ) ) )
    writetoCOGS(COGS,theTime)
    print( dim(dbGetQuery(conKO , "SELECT * FROM SWIRE.BALANCINGCOGS" ) ) )
    
    dbDisconnect(conKO)
    
    print("Success!!")
    
    Sys.sleep(120)
    
    
  }
  
}
