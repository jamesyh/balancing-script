# Test Push
testColsplit = reshape::colsplit

# BW Functions
NewReadTable = function (con, saptable, options = list(), fields = list(), delimiter = ";", 
                         nrows = -1, skip = 0) 
{
  if (!RSAPValidHandle(con)) 
    stop("argument is not a valid RSAP con")
  library(reshape)
  parms <- list(DELIMITER = delimiter, QUERY_TABLE = saptable, 
                OPTIONS = list(TEXT = options), FIELDS = list(FIELDNAME = fields), 
                ROWSKIPS = skip)
  if (nrows != -1) {
    parms[["ROWCOUNT"]] <- nrows
  }
  res <- RSAPInvoke(con, "RFC_READ_TABLE", parms)
  flds <- sub("\\s+$", "", res$FIELDS$FIELDNAME)
  data <- NULL
  if (length(res$DATA$WA) == 0) {
    data <- data.frame()
  }
  else {
    data <- data.frame(res$DATA, reshape::colsplit(res$DATA$WA, split = delimiter, 
                                                   names = flds))
  }
  for (i in 1:length(flds)) {
    f <- flds[i]
    typ <- res$FIELDS$TYPE[i]
    if (typ == 'N' || typ == 'I' || typ == 'P') {
      data[[f]] <- as.numeric(unlist(lapply(data[[f]], FUN=function (x) { sub("([0-9.]+)-$", "-\\1", gsub("[^\\d\\.\\-]", "", x, perl=TRUE)) })));
      data[[f]][is.na(data[[f]])] <- 0 
    } else {
      data[[f]] <- sub("\\s+$", "", data[[f]]);
    }
    
  }
  data$WA <- NULL
  return(data)
}



# Grabbing MOst Recent Info for pivot table

mostRecent = function(PV) {
  
  PV$MOSTRECENT = "NO"
  
  index = unique(PV[,c("YEAR","PERIOD")])
  
  
  index = index[order(as.numeric(index$PERIOD) ) , ]
  
  for(i in 1:nrow(index)){
    
    theMax  = max(PV[PV$YEAR == index[i,"YEAR"] & PV$PERIOD == index[i,"PERIOD"] , "DATE" ] )
    theMaxRan  = max(PV[PV$YEAR == index[i,"YEAR"] & PV$PERIOD == index[i,"PERIOD"] & PV$DATE == theMax, "TIMESRANTODAY" ] )
    
    
    PV[PV$YEAR == index[i,"YEAR"] & PV$PERIOD == index[i,"PERIOD"] & PV$DATE == theMax & PV$TIMESRANTODAY == theMaxRan , "MOSTRECENT" ] = "YES"
    
    
  }
  
  return(PV)
  
}

# Generic form
'%=%' = function(l, r, ...) UseMethod('%=%')

# Binary Operator
'%=%.lbunch' = function(l, r, ...) {
  Envir = as.environment(-1)
  
  if (length(r) > length(l))
    warning("RHS has more args than LHS. Only first", length(l), "used.")
  
  if (length(l) > length(r))  {
    warning("LHS has more args than RHS. RHS will be repeated.")
    r <- extendToMatch(r, l)
  }
  
  for (II in 1:length(l)) {
    do.call('<-', list(l[[II]], r[[II]]), envir=Envir)
  }
}

# Used if LHS is larger than RHS
extendToMatch <- function(source, destin) {
  s <- length(source)
  d <- length(destin)
  
  # Assume that destin is a length when it is a single number and source is not
  if(d==1 && s>1 && !is.null(as.numeric(destin)))
    d <- destin
  
  dif <- d - s
  if (dif > 0) {
    source <- rep(source, ceiling(d/s))[1:d]
  }
  return (source)
}

# Grouping the left hand side
g = function(...) {
  List = as.list(substitute(list(...)))[-1L]
  class(List) = 'lbunch'
  return(List)
}


# QDATE Function

QDATE = function(CURRENT = T){
  
  conKO <- dbConnect(odbc(), "senior",
                     uid = usr , 
                     pwd = pass)
  
  
  
  
  QDATE = dbGetQuery(conKO , "SELECT PERIOD , USADAT FROM SWIRE.QDATE" )
  QDATE$YEAR = year(QDATE$USADAT)
  
  PERIOD = QDATE[ QDATE$USADAT == as.Date( substr(Sys.time() , 1 , 10) ) , "PERIOD" ]
  YEAR = QDATE[ QDATE$USADAT == as.Date( substr(Sys.time() , 1 , 10) ) , "YEAR" ]
  theDATE = QDATE[ QDATE$USADAT == as.Date( substr(Sys.time() , 1 , 10) ) , "USADAT" ]
  
  
  dbDisconnect(conKO)
  
  if(CURRENT == F) {
    if( PERIOD == 1) PERIOD = 12
    if( PERIOD != 1) PERIOD = PERIOD - 1
  }
  
  return(c(PERIOD,YEAR,as.character(theDATE) ))
  
}



BW = function(YEAR,PERIOD) {
  
  

  conBW <- RSAPConnect(ashost=Address, sysnr=Num,
                       client=Num, user=usr,
                       passwd=pass, lang="EN")
  
  
  YearPeriod = paste0(YEAR , "0" , PERIOD)
  if( nchar(PERIOD) == 1 ) YearPeriod = paste0(YEAR , "00" , PERIOD)
  
  
  
  cube = "ZCA_C11"
  chars = list("0COSTCENTER","0COSTELMNT",'0VTYPE','0VERSION')
  kfigures = list("0AMOUNT","0QUANTITY")
  options=list(CHANM=list('0FISCPER'),SIGN=list('I'), 
               COMPOP=list('EQ'), LOW=list(YearPeriod))
  
  
  
  ref_date <- format(Sys.time(), "%Y%m%d")
  caliases <- as.list(sub("^\\d+", "", chars))
  kaliases <- as.list(sub("^\\d+", "", kfigures))
  parms <- list(I_INFOCUBE = cube, I_REFERENCE_DATE = ref_date, 
                I_T_CHA = list(CHANM = chars, CHAALIAS = caliases), I_T_KYF = list(KYFNM = kfigures, 
                                                                                   KYFALIAS = kaliases), I_T_RANGE = options)
  res <- RSAPInvoke(conBW, "RSDPL_CUBE_DATA_READ", parms)
  other <- NewReadTable(conBW, res$E_TABLENAME)
  
  cube = other
  
  cube = cube[cube$VTYPE == 10 , ]
  cube = cube[cube$VERSION == 0 , ]
  cube = cube[!is.na(cube[,1 ]) , ]
  
  time = Sys.time()
  cube$TYPE = as.numeric( substr(cube$COSTELMNT,1,4) )
  cube$COSTCENTER = as.numeric( substr(cube$COSTCENTER,1,3) )
  # print( paste("BW",time - Sys.time() ) )
  
  # 
  # 
  # draper = cube[cube$COSTCENTER == 111 , ]
  # draper[draper$COSTELMNT == 899301253 , ]
  
  
  
  huh = cube[cube$COSTELMNT %in% c(899301253,	899301254,	899301256,	899301260,	899301261,	899301262,	899301265,	899301266,	899301267,	899301268,	
                                   899301269,	899301275,	899301276,	899301776,	899301777,	899301400,	899301407,	899301408,	899301420,	899301427,	
                                   899301429,	899301431,	899301433,	899301435,	899301451,	899301453,	899301455,	899301459,	899301461,	899301462,	
                                   899301463,	899301464,	899301471,	899301478,	899301480,	899301481,	899301482,	899301483,	899301484,	899301485,	
                                   899301486,	899301487,	899301501,	899301503,	899301505,	899301507,	899301517,	899301518,	899301520,	899301521,	
                                   899301522,	899301524,	899301525,	899301526,	899301527,	899301530,	899301552,	899301553,	899301557,	899301558,	
                                   899301559,	899301560,	899301562,	899301563,	899301565,	899301566,	899301568,	899301590,	899301600,	899301603,	
                                   899301608,	899301614,	899301619,	899301620,	899301624,	899301628,	899301629,	899301630,	899301202,	899301204,	
                                   899301205,	899301206,	899301207,	899301208,	899301211,	899301212,	899301230,	899301231,	899301233,	899301234,	
                                   899301237,	899301245,	899301307,	899301308,	899301310,	899301316,	899301318,	899301320,	899301301,  899301257,
                                   899301259, 899301610,  899301502,  899301465,  899301607,  899301606,  899301623,  899301601) , ]
  
  COGS = cube[cube$COSTELMNT %in% c(505201202,	505201204,	505201205,	505201206,	505201207,	505201208,	505201211,	505201212,	505201230,	505201231,	
                                    505201233,	505201234,	505201237,	505201245,	505201253,	505201254,	505201256,	505201260,	505201261,	505201262,	
                                    505201265,	505201266,	505201267,	505201268,	505201269,	505201275,	505201276,	505201301,	505201307,	505201308,	
                                    505201310,	505201316,	505201318,	505201320,	505201400,	505201407,	505201408,	505201420,	505201427,	505201429,	
                                    505201431,	505201433,	505201435,	505201451,	505201453,	505201455,	505201459,	505201461,	505201462,	505201463,	
                                    505201464,	505201471,	505201478,	505201480,	505201481,	505201482,	505201483,	505201484,	505201485,	505201486,	
                                    505201487,	505201501,	505201503,	505201505,	505201507,	505201517,	505201518,	505201520,	505201521,	505201522,	
                                    505201524,	505201525,	505201526,	505201527,	505201530,	505201553,	505201558,	505201559,	505201560,	505201562,	
                                    505201563,	505201565,	505201566,	505201568,	505201590,	505201600,	505201603,	505201608,	505201614,	505201619,	
                                    505201620,	505201624,	505201628,	505201629,	505201630,	505201776,	505201777,  505201257,  505201259,  505201610,
                                    505201502,  505401502,  505201552,  505201465,  505201310) , ]
  
  NSI = cube[cube$COSTELMNT %in% c(305701202,	305701204,	305701205,	305701206,	305701207,	305701208,	305701211,	305701212,	305701230,	305701231,	
                                   305701233,	305701234,	305701237,	305701245,	305701253,	305701254,	305701256,	305701260,	305701261,	305701262,	
                                   305701265,	305701266,	305701267,	305701268,	305701269,	305701275,	305701276,	305701301,	305701307,	305701308,	
                                   305701310,	305701316,	305701318,	305701320,	305701400,	305701407,	305701408,	305701420,	305701427,	305701429,	
                                   305701431,	305701433,	305701435,	305701451,	305701453,	305701455,	305701459,	305701461,	305701462,	305701463,	
                                   305701464,	305701471,	305701478,	305701480,	305701481,	305701482,	305701483,	305701484,	305701485,	305701486,	
                                   305701487,	305701501,	305701503,	305701505,	305701507,	305701517,	305701518,	305701520,	305701521,	305701522,	
                                   305701524,	305701525,	305701526,	305701527,	305701530,	305701553,	305701558,	305701559,	305701560,	305701562,	
                                   305701563,	305701565,	305701566,	305701568,	305701590,	305701600,	305701603,	305701608,	305701614,	305701619,	
                                   305701620,	305701624,	305701628,	305701629,	305701630,	405506202,	405506204,	405506205,	405506206,	405506207,	
                                   405506208,	405506211,	405506212,	405506230,	405506231,	405506233,	405506234,	405506237,	405506245,	405506253,	
                                   405506254,	405506256,	405506260,	405506261,	405506262,	405506265,	405506266,	405506267,	405506268,	405506269,	
                                   405506275,	405506301,	405506307,	405506308,	405506310,	405506316,	405506318,	405506320,	405506400,	405506407,	
                                   405506408,	405506420,	405506427,	405506429,	405506431,	405506433,	405506435,	405506451,	405506453,	405506455,	
                                   405506459,	405506461,	405506462,	405506463,	405506464,	405506471,	405506478,	405506480,	405506481,	405506482,	
                                   405506483,	405506484,	405506485,	405506486,	405506487,	405506501,	405506503,	405506505,	405506507,	405506517,	
                                   405506518,	405506520,	405506521,	405506522,	405506524,	405506525,	405506526,	405506527,	405506530,	405506553,	
                                   405506558,	405506559,	405506560,	405506562,	405506563,	405506565,	405506566,	405506568,	405506590,	405506600,	
                                   405506603,	405506608,	405506614,	405506619,	405506620,	405506624,	405506628,	405506629,	405506630,  405506257,
                                   405506259, 405506502,  405506610,  305701610,  305701502,  305701259,  305701257,  305701552,  305701465) , ]
  
  
  
  
  
  
  # 
  # 
  # 
  # draper = huh[huh$COSTCENTER == 111 , ]
  # draper[draper$COSTELMNT == 899301253 , ]
  # 
  # 
  
  BWNSI = tapply(NSI$AM, NSI$COSTCENTER, sum)
  BWNSI = as.data.frame(BWNSI)
  names(BWNSI)[1] = "NSI"
  BWNSI$CostCenter = rownames(BWNSI)
  rownames(BWNSI) = 1:nrow(BWNSI)
  
  BWCOGS = tapply(COGS$AM, COGS$COSTCENTER, sum)
  BWCOGS = as.data.frame(BWCOGS)
  names(BWCOGS)[1] = "COGS"
  BWCOGS$CostCenter = rownames(BWCOGS)
  rownames(BWCOGS) = 1:nrow(BWCOGS)
  
  BW = huh
  BWVolume = tapply( BW$AMOUNT , BW$COSTCENTER , sum )
  BWVolume = as.data.frame(BWVolume)
  names(BWVolume)[1] = "Volume"
  BWVolume$CostCenter = rownames(BWVolume)
  rownames(BWVolume) = 1:nrow(BWVolume)
  
  BWVolume = merge(BWVolume , BWCOGS , by = "CostCenter" , all = T)
  
  BWVolume = merge(BWVolume , BWNSI , by = "CostCenter" , all = T)
  
  return(BWVolume)
  
}


# SENIOR Function

KO = function(YEAR,PERIOD,CURRENTPERIOD) {
  

  conKO <- dbConnect(odbc(), "senior",
                     uid = usr , 
                     pwd = pass)
  
  
  YearPeriod = 'CONA.KOEXT'
  
  if(CURRENTPERIOD == FALSE){
    YearPeriod = paste0( "BASSD.", "KOEXT" , substring(YEAR,3,4) ,  PERIOD)
    if( nchar(PERIOD) == 1 ) YearPeriod = paste0( "BASSD.KOEXT" , substring(YEAR,3,4) , "0" ,  PERIOD)
  }
  
  
  
  
  SCRIPT = paste('SELECT RSMATL , RSWHR , RSAVV# , RSDAT , RSART , RSMASG , SUM(RSQTY) as RSQTY , SUM(RSSALTOT) AS RSSALTOT , 
                 SUM(RSCOGTOT) AS RSCOGTOT , SUM(RSAVVTOT) AS RSAVVTOT , SUM(RSSPCQTY) AS RSSPCQTY , SUM(RSEXAM) as RSEXAM   
                 FROM ',YearPeriod,' GROUP BY RSMATL , RSWHR , RSAVV# , RSDAT , RSART , RSMASG' )
  
  time = Sys.time()
  DF = dbGetQuery(conKO , SCRIPT )
  # print( paste("KO", time - Sys.time() ) )
  
  DF$COGSINDICATOR = 0
  DF$DISCINDICATOR = 0
  
  DF[DF$`RSAVV#` %in% c(76,78,79,80,81,82,83,84) , "COGSINDICATOR" ] = 1
  DF[DF$`RSAVV#` %in% c(10,11,13,22,23,44,63) , "DISCINDICATOR" ] = 1
  
  
  DF$COGS = DF$RSAVVTOT * DF$COGSINDICATOR
  DF$DISC = DF$RSAVVTOT * DF$DISCINDICATOR
  
  plant = dbGetQuery(conKO , 'SELECT WHS , PLNT FROM SWIRE.PLANT' )
  
  step1 = merge(DF , plant , by.x = "RSWHR" , by.y = "WHS")
  
  am01 = dbGetQuery(conKO , 'SELECT AM01_ARTNUM , AM01_ARTCNVFAC2 , AMBCPPGRP , COMATASGP FROM BASDBC1.AM01ETD
                    WHERE AM01_EFTDAT = 1999999')
  
  
  KOEXTT = merge(step1,am01 , by.x = "RSART" , by.y = "AM01_ARTNUM")
  
  
  # KOEXTT$NSI = round( KOEXTT$RSSALTOT - KOEXTT$DISC, digits = 2)
  
  # KOEXTT$GL = KOEXTT$RSQTY * KOEXTT$AM01_ARTCNVFAC2
  KOEXTT$GL = floor( KOEXTT$RSQTY * KOEXTT$AM01_ARTCNVFAC2 * 10 ) / 10
  
  KOEXTT$GL = KOEXTT$RSQTY * KOEXTT$AM01_ARTCNVFAC2
  
  names(KOEXTT)[names(KOEXTT) == "RSAVV#"] = "RSAVV"
  
  KOEXTTHANA = sqldf("SELECT PLNT  , SUM(RSQTY) VOLUMEPHYSICAL , SUM(RSSALTOT) - SUM(DISC) NSI , SUM(COGS) COGS , SUM(RSSALTOT) AS RSSALTOT , SUM(DISC) DISC
                     FROM KOEXTT
                     WHERE RSMASG IN (\'11\'  , \'18\' , \'41\')
                     GROUP BY PLNT ")
  
  KOEXTTBW = sqldf("SELECT PLNT , SUM(GL) VOLUMEGLUNIT , SUM(RSSALTOT) - SUM(DISC) NSI , SUM(COGS) COGS
                   FROM KOEXTT
                   WHERE AMBCPPGRP IN (\'1999\' , \'2999\' , \'3999\' , \'4999\' , \'5999\' , \'6999\')
                   GROUP BY PLNT")
  
  
  KOEXTT = sqldf("SELECT TRIM(KOEXTTHANA.PLNT) AS PLNT ,  KOEXTTHANA.VOLUMEPHYSICAL , KOEXTTBW.VOLUMEGLUNIT ,
                 KOEXTTHANA.NSI AS NSI , KOEXTTHANA.COGS AS COGS
                 FROM KOEXTTHANA
                 LEFT JOIN KOEXTTBW ON KOEXTTHANA.PLNT = KOEXTTBW.PLNT")
  
  return(KOEXTT)
  
  
}

KONEW = function(YEAR,PERIOD,CURRENTPERIOD) {
  

  
  conKO <- dbConnect(odbc(), "senior",
                     uid = usr , 
                     pwd = pass)
  
  
  YearPeriod = 'BASSD.KOEXTT'
  
  if(CURRENTPERIOD == FALSE){
    YearPeriod = paste0( "BASSD.", "KOEXT" , substring(YEAR,3,4) ,  PERIOD)
    if( nchar(PERIOD) == 1 ) YearPeriod = paste0( "BASSD.KOEXT" , substring(YEAR,3,4) , "0" ,  PERIOD)
  }
  
  
  
  
  
  
  # SCRIPT = paste('SELECT PLNT ,
  #                SUM(RSQTY) as VOLUMEPHYSICAL , SUM(RSQTY*BASDBC1.AM01ETD.AM01_ARTCNVFAC2) as VOLUMEGLUNIT ,
  #                SUM(RSSALTOT) - SUM(CASE WHEN RSAVV# IN (\'10\',\'11\',\'13\',\'22\',\'23\',\'44\',\'63\') THEN RSAVVTOT
  #                ELSE 0 END) AS NSIAVV , SUM(RSSALTOT) -  SUM(RSEXAM) NSIRSEXAM ,
  #                SUM( CASE WHEN RSAVV# IN (\'76\',\'78\',\'79\',\'80\',\'81\',\'82\',\'83\',\'84\')  THEN RSAVVTOT
  #                ELSE NULL END) AS COGS 
  #                FROM ',YearPeriod,' KO , SWIRE.PLANT , BASDBC1.AM01ETD , SWIRE.RSADJF
  #                WHERE KO.RSWHR = SWIRE.PLANT.WHS
  #                AND KO.RSART = BASDBC1.AM01ETD.AM01_ARTNUM
  #                AND KO.RSATP = SWIRE.RSADJF.ADJTYP
  #                AND SWIRE.RSADJF.ADJCLS IN (\'0\',\'1\')
  #                AND KO.RSCLS BETWEEN \'01\' AND \'50\'
  #                AND AM01_EFTDAT = 1999999
  #                GROUP BY PLNT
  #                ORDER BY PLNT')
  
  SCRIPT = paste('SELECT PLNT ,
                 SUM(RSQTY) as VOLUMEPHYSICAL , SUM(RSQTY*BASDBC1.AM01ETD.AM01_ARTCNVFAC2) as VOLUMEGLUNIT ,
                 SUM(RSSALTOT) - SUM(CASE WHEN RSAVV# IN (\'10\',\'11\',\'13\',\'22\',\'23\',\'44\',\'63\') THEN RSAVVTOT
                 ELSE 0 END) AS NSIAVV , SUM(RSSALTOT) -  SUM(RSEXAM) NSIRSEXAM ,
                 SUM(RSCOGTOT) AS COGS 
                 FROM ',YearPeriod,' KO , SWIRE.PLANT , BASDBC1.AM01ETD , SWIRE.RSADJF
                 WHERE KO.RSWHR = SWIRE.PLANT.WHS
                 AND KO.RSART = BASDBC1.AM01ETD.AM01_ARTNUM
                 AND KO.RSATP = SWIRE.RSADJF.ADJTYP
                 AND SWIRE.RSADJF.ADJCLS IN (\'0\',\'1\')
                 AND KO.RSCLS BETWEEN \'01\' AND \'50\'
                 AND AM01_EFTDAT = 1999999
                 GROUP BY PLNT
                 ORDER BY PLNT')
  
  KOEXTT1 = dbGetQuery(conKO , SCRIPT )
  
  SCRIPT = paste('SELECT PLNT ,
                 SUM(RSQTY) as VOLUMEPHYSICAL , SUM(RSQTY*BASDBC1.AM01ETD.AM01_ARTCNVFAC2) as VOLUMEGLUNIT ,
                 SUM(RSSALTOT) - SUM(CASE WHEN RSAVV# IN (\'10\',\'11\',\'13\',\'22\',\'23\',\'44\',\'63\') THEN RSAVVTOT
                 ELSE 0 END) AS NSIAVV , SUM(RSSALTOT) -  SUM(RSEXAM) NSIRSEXAM ,
                 SUM(RSCOGTOT) AS COGS 
                 FROM ',YearPeriod,' KO , SWIRE.PLANT , BASDBC1.AM01ETD , SWIRE.RSADJF
                 WHERE KO.RSWHR = SWIRE.PLANT.WHS
                 AND KO.RSART = BASDBC1.AM01ETD.AM01_ARTNUM
                 AND KO.RSATP = SWIRE.RSADJF.ADJTYP
                 AND SWIRE.RSADJF.ADJCLS IN (\'0\',\'1\')
                 AND KO.RSCLS BETWEEN \'W1\' AND \'W3\'
                 AND AM01_EFTDAT = 1999999
                 GROUP BY PLNT
                 ORDER BY PLNT')
  
  KOEXTT2 = dbGetQuery(conKO , SCRIPT )
  
  KOEXTT = rbind(KOEXTT1,KOEXTT2)
  
  KOEXTT = sqldf("select PLNT, sum(VOLUMEPHYSICAL) VOLUMEPHYSICAL,sum(VOLUMEGLUNIT) VOLUMEGLUNIT, sum(NSIAVV) NSIAVV,
                 sum(NSIRSEXAM) NSIRSEXAM,sum(COGS) COGS from KOEXTT GROUP BY PLNT")
  
  KOEXTT$PLNT = trimws(KOEXTT$PLNT)
  
  return(KOEXTT)
  
  
}

KOISSUE = function() {
  

  conKO <- dbConnect(odbc(), "senior",
                     uid = usr , 
                     pwd = pass)
  
  
  
  
  QDATE = dbGetQuery(conKO , "SELECT PERIOD , USADAT FROM SWIRE.QDATE" )
  QDATE$YEAR = year(QDATE$USADAT)
  
  PERIOD = QDATE[ QDATE$USADAT == as.Date( substr(Sys.time() , 1 , 10) ) , "PERIOD" ]
  YEAR = QDATE[ QDATE$USADAT == as.Date( substr(Sys.time() , 1 , 10) ) , "YEAR" ]
  theDATE = QDATE[ QDATE$USADAT == as.Date( substr(Sys.time() , 1 , 10) ) , "USADAT" ]
  
  
  BASSD = paste0( "BASSD.", "KOEXT" , substring(YEAR,3,4) ,  PERIOD - 1)
  if( nchar(PERIOD - 1) == 1 ) BASSD = paste0( "BASSD.KOEXT" , substring(YEAR,3,4) , "0" ,  PERIOD - 1)
  
  
  
  
  CUR = dbGetQuery(conKO ,"SELECT RSPERD , SUM(RSQTY) RSQTY 
                   FROM CONA.KOEXT 
                   WHERE RSMASG IN (\'11\'  , \'18\' , \'41\')
                   GROUP BY RSPERD")
  
  
  LAS = dbGetQuery(conKO ,paste("SELECT RSPERD , SUM(RSQTY) RSQTY 
                                FROM ",BASSD,"WHERE RSMASG IN (\'11\'  , \'18\' , \'41\') GROUP BY RSPERD"))
  
  
  curper = paste0( YEAR , "0"  ,  PERIOD )
  if( nchar(PERIOD) == 1 ) curper = paste0( YEAR , "00"  ,  PERIOD )
  
  
  
  issue = CUR[CUR$RSPERD != curper , "RSQTY" ]
  
  
  
  dbDisconnect(conKO)
  
  return(list(issue,CUR))
  
}

ZRTLRS = function(PERIOD , YEAR = 2018) {
  

  
  conKO <- dbConnect(odbc(), "senior",
                     uid = usr , 
                     pwd = pass)
  
  
  
  
  if(YEAR == 2018){
    
    
    
    SCRIPT = paste('SELECT PLNT , RSWHR , sum(VOLPHYBCPP) ZRVOLUME,sum(VOLGLUBCPP) ZRGLVOLUME , sum(NSISM) ZRNSI , sum(COGBCPP) ZRCOGS
                   FROM SWIRE.ZRTLRS ZRTLRS , SWIRE.PLANT PLANT
                   WHERE ZRTLRS.RSWHR = PLANT.WHS
                   AND PERIOD IN ( ',PERIOD,')
                   AND PRDDESC NOT IN ( \'OTH CO2 FULL \',\'OTH CUPS\',\'OTH LIDS\',\'OTH STRAWS\' )              
                   GROUP BY PLNT , RSWHR
                   ORDER BY PLNT')
    
  }
  
  if(YEAR == 2017){
    
    
    
    SCRIPT = paste('SELECT PLNT , RSWHR , sum(VOLPHYBCPP) ZRVOLUME,sum(VOLGLUBCPP) ZRGLVOLUME , sum(NSISM) ZRNSI , sum(COGBCPP) ZRCOGS
                   FROM SWIRE.ZRTLRS2017 ZRTLRS , SWIRE.PLANT PLANT
                   WHERE ZRTLRS.RSWHR = PLANT.WHS
                   AND PERIOD IN ( ',PERIOD,')
                   AND PRDDESC NOT IN ( \'OTH CO2 FULL \',\'OTH CUPS\',\'OTH LIDS\',\'OTH STRAWS\' )              
                   GROUP BY PLNT , RSWHR
                   ORDER BY PLNT')
    
  }
  
  ZRTLRS = dbGetQuery(conKO , SCRIPT )
  ZRTLRS = ZRTLRS[ZRTLRS$ZRVOLUME != 0 , ]
  
  ZRTLRS$PLNT = trimws(ZRTLRS$PLNT)
  
  return(ZRTLRS)
}


# HANA Function

COPA = function(YEAR,PERIOD) {
  

  
  conHANA <- dbConnect(odbc(), "HANA",
                       uid = usr , 
                       pwd = pass)
  
  YearPeriod = paste0(YEAR , "0" , PERIOD)
  if( nchar(PERIOD) == 1 ) YearPeriod = paste0(YEAR , "00" , PERIOD)
  
  SCRIPT = paste0('
                  SELECT
                  "ProfitCenterText" AS "ProfitCenterName",
                  "ProfitCenter" AS "ProfitCenterCode",
                  sum("VV998QPhysicalCases") AS "VV998QPhysicalCases",
                  sum("ERLOSRevenue"),
                  sum("VV004UpchargeChannel" ),
                  sum("VV076COGSPurchasedFG"),
                  sum("VV078COGSConcentrate"), 
                  sum("VV079COGSIngredients"), 
                  sum("VV080COGSContainers"), 
                  sum("VV081COGSPackaging"),
                  sum("VV082COGSLaborActivity"),
                  sum("VV083COGSOtherOverheads"),
                  sum("VV084Reserve50CGS" ),
                  sum("VV010EDVChannelDiscount"),
                  sum("VV011EDVCustomer"),
                  sum("VV013EmployeeDiscounts"),
                  sum("VV022NMDOTHER") ,
                  sum("VV044CRMTPMOnInv"),
                  sum("VV023VolumeDiscount"),
                  sum("VV063Reserve35Swire")
                  FROM "_SYS_BIC"."cona-reporting.profitability/Q_CA_M_CopaPowerView" (\'PLACEHOLDER\' = (\'$$IP_CurrencyType$$\',
                  \'10\')) 
                  WHERE (("FiscalYearPeriod" IN (',"'", YearPeriod ,"'",') )) 
                  AND "MaterialAccountAssignmentGroup" IN ( \'11\' , \'18\' , \'41\' )                
                  GROUP BY 
                  "ProfitCenterText",
                  "ProfitCenter"
                  ' )
  
  time = Sys.time()
  
  Hana = dbGetQuery(conHANA , SCRIPT)
  
  Hana$Wholesale = Hana$`SUM(ERLOSRevenue)` + Hana$`SUM(VV004UpchargeChannel)`
  Hana$COGS = Hana$`SUM(VV076COGSPurchasedFG)` + Hana$`SUM(VV078COGSConcentrate)` + 
    Hana$`SUM(VV079COGSIngredients)` + Hana$`SUM(VV080COGSContainers)` + 
    Hana$`SUM(VV081COGSPackaging)` + Hana$`SUM(VV082COGSLaborActivity)` +
    Hana$`SUM(VV083COGSOtherOverheads)` + Hana$`SUM(VV084Reserve50CGS)`
  Hana$DISC = Hana$`SUM(VV010EDVChannelDiscount)` + Hana$`SUM(VV011EDVCustomer)` +
    Hana$`SUM(VV013EmployeeDiscounts)` + Hana$`SUM(VV022NMDOTHER)` + 
    Hana$`SUM(VV044CRMTPMOnInv)` + Hana$`SUM(VV023VolumeDiscount)` + 
    Hana$`SUM(VV063Reserve35Swire)`
  Hana$NSI = Hana$Wholesale - Hana$DISC
  Hana$Volume = Hana$VV998QPhysicalCases
  Hana$ProfitCenterCode = substring( Hana$ProfitCenterCode , 5 , 7)
  
  HANA = Hana[ , c("ProfitCenterName" , "ProfitCenterCode" , "Volume" ,
                   "Wholesale" , "COGS" , "DISC" , "NSI")]
  
  dbDisconnect(conHANA)
  
  HANA = sqldf("SELECT TRIM(ProfitCenterCode) AS ProfitCenterCode , ProfitCenterName , Volume , Wholesale , COGS , DISC , NSI  
               FROM HANA")
  return(HANA)
  
}

COPACurrent = function(KOEXTT) {
  
  if( substr( file.info( "J:/IS/Reconciliation/Balancing R/workspace.RData" )$ctime , 1 , 10) == substr( Sys.Date() , 1 , 10) ) {
    
    load(file =  "J:/IS/Reconciliation/Balancing R/workspace.RData" )
    
    done = as.data.frame( tapply(catch$Volume , catch$Number , sum) )
    
    g(issue,CUR) %=% KOISSUE()
    
    KOTOT = sum(KOEXTT$VOLUMEPHYSICAL)
    if( length( table(CUR$RSPERD) ) != 1 ) KOTOT = sum(KOEXTT$VOLUMEPHYSICAL) - issue
    
    num = which( abs( done - KOTOT ) == min( abs( done - KOTOT ) ) )[1]
    
    
    HANA = catch[catch$Number == num , ]
    
    
    
  }
  
  else{
    
    source("RCODE/HANA/HANACHECKONCE.R")
    
    load(file =  "J:/IS/Reconciliation/Balancing R/workspace.RData" )
    
    done = as.data.frame( tapply(catch$Volume , catch$Number , sum) )
    
    g(issue,CUR) %=% KOISSUE()
    
    KOTOT = sum(KOEXTT$VOLUMEPHYSICAL)
    if( length( table(CUR$RSPERD) ) != 1 ) KOTOT = sum(KOEXTT$VOLUMEPHYSICAL) - issue
    
    num = which( abs( done - KOTOT ) == min( abs( done - KOTOT ) ) )[1]
    
    
    HANA = catch[catch$Number == num , ]
    
  }
  
  return(HANA)
  
}


# MM Function

print("DGTEAM")
MMTXT = function(PE = T) {
  
  system( '"C:\\MMUXT6.2\\salcli.exe" -RM"Physical Balancing MRC" -DS"Swire_Meta" -U"DGTEAM"' )
  system( '"C:\\MMUXT6.2\\salcli.exe" -RM"GL Unit Balancing MRC" -DS"Swire_Meta" -U"DGTEAM"' )
  system( '"C:\\MMUXT6.2\\salcli.exe" -RM"Physical Balancing" -DS"Swire_Meta" -U"DGTEAM"' )
  system( '"C:\\MMUXT6.2\\salcli.exe" -RM"GL Unit Balancing" -DS"Swire_Meta" -U"DGTEAM"' )
  
  # PE = T
  
  if( PE == T ){
    GLMM = read.csv("J:/IS/BW_Balance/James Balancing/Data/GL MM PE.txt" )
    PhyMM = read.csv("J:/IS/BW_Balance/James Balancing/Data/Physical MM PE.txt" )
  }
  
  else{
    GLMM = read.csv("J:/IS/BW_Balance/James Balancing/Data/GL MM.txt" )
    PhyMM = read.csv("J:/IS/BW_Balance/James Balancing/Data/Physical MM.txt" )
  }
  
  GLMM = GLMM[GLMM$Act.Cost.Cntr != "Totals" , ]
  PhyMM = PhyMM[PhyMM$Act.Cost.Cntr != "Totals" , ]
  
  names(GLMM) = gsub("\\.","",names(GLMM))
  names(PhyMM) = gsub("\\.","",names(PhyMM))
  
  names(GLMM) = gsub("\\[","",names(GLMM))
  names(PhyMM) = gsub("\\[","",names(PhyMM))
  
  names(GLMM) = gsub("\\]","",names(GLMM))
  names(PhyMM) = gsub("\\]","",names(PhyMM))
  
  names(GLMM) = gsub("\\$","",names(GLMM))
  names(PhyMM) = gsub("\\$","",names(PhyMM))
  
  #  GLMM = GLMM[,names(GLMM) != "NSISimple"]
  
  names(GLMM)[names(GLMM) == "ConaNSISimpleCona_Sales"] = "CONANSISimple"
  names(GLMM)[names(GLMM) == "NSI"] = "MMNSI"
  
  valToRet = list(GLMM,PhyMM)
  
  return(valToRet)
}


MM = function(PE = T) {
  
  
  
  
  if( PE == T ){
    
    GLMM = openxlsx::read.xlsx("J:/IS/BW_Balance/James Balancing/Data/GL MM PE.xlsx" , sheet = 1)
    PhyMM = read.xlsx("J:/IS/BW_Balance/James Balancing/Data/Physical MM PE.xlsx" , sheet= 1)
  }
  
  else{
    GLMM = read.xlsx("J:/IS/BW_Balance/James Balancing/Data/GL MM.xlsx" , sheet= 1)
    PhyMM = read.xlsx("J:/IS/BW_Balance/James Balancing/Data/Physical MM.xlsx" , sheet = 1)
  }
  
  GLMM = GLMM[GLMM$Act.Cost.Cntr != "Totals" , ]
  PhyMM = PhyMM[PhyMM$Act.Cost.Cntr != "Totals" , ]
  
  names(GLMM) = gsub("\\.","",names(GLMM))
  names(PhyMM) = gsub("\\.","",names(PhyMM))
  
  names(GLMM) = gsub("\\[","",names(GLMM))
  names(PhyMM) = gsub("\\[","",names(PhyMM))
  
  names(GLMM) = gsub("\\]","",names(GLMM))
  names(PhyMM) = gsub("\\]","",names(PhyMM))
  
  names(GLMM) = gsub("\\$","",names(GLMM))
  names(PhyMM) = gsub("\\$","",names(PhyMM))
  
  #  GLMM = GLMM[,names(GLMM) != "NSISimple"]
  
  names(GLMM)[names(GLMM) == "ConaNSISimpleCona_Sales"] = "CONANSISimple"
  names(GLMM)[names(GLMM) == "NSI"] = "MMNSI"
  
  valToRet = list(GLMM,PhyMM)
  
  
  
  
  return(valToRet)
}

runMM = function(){
  
  time = file.info("J:/IS/BW_Balance/James Balancing/Data/GL MM.xlsx")$mtime
  
  
  
  system( '"C:\\MMUXT6.2\\salcli.exe" -RM"Physical Balancing MRC" -DS"Swire_Meta" -U"DGTEAM"' , wait = F )
  system( '"C:\\MMUXT6.2\\salcli.exe" -RM"GL Unit Balancing MRC" -DS"Swire_Meta" -U"DGTEAM"' , wait = F )
  system( '"C:\\MMUXT6.2\\salcli.exe" -RM"Physical Balancing" -DS"Swire_Meta" -U"DGTEAM"' , wait = F )
  system( '"C:\\MMUXT6.2\\salcli.exe" -RM"GL Unit Balancing" -DS"Swire_Meta" -U"DGTEAM"' , wait = F )
  
  
  theTime = Sys.time() + 120
  
  
  theMM = F
  
  while(time == file.info("J:/IS/BW_Balance/James Balancing/Data/GL MM.xlsx")$mtime ){
    
    if( !(Sys.time() < theTime) ) {
      theMM = T
      break
      
    }
    Sys.sleep(10)
  }
  
  return(theMM)
}

MMPast = function(PE = T) {
  
  
  
  
  if( PE == T ){
    
    # system( '"C:\\MMUXT6.2\\salcli.exe" -RM"Physical Balancing MRC" -DS"Swire_Meta" -U"JHILL"' )
    # system( '"C:\\MMUXT6.2\\salcli.exe" -RM"GL Unit Balancing MRC" -DS"Swire_Meta" -U"JHILL"' )
    # system( '"C:\\MMUXT6.2\\salcli.exe" -RM"Physical Balancing" -DS"Swire_Meta" -U"JHILL"' )
    # system( '"C:\\MMUXT6.2\\salcli.exe" -RM"GL Unit Balancing" -DS"Swire_Meta" -U"JHILL"' )
    # 
    
    GLMM = openxlsx::read.xlsx("J:/IS/BW_Balance/James Balancing/Data/GL MM PE.xlsx" , sheet = 1)
    PhyMM = read.xlsx("J:/IS/BW_Balance/James Balancing/Data/Physical MM PE.xlsx" , sheet= 1)
  }
  
  else{
    GLMM = read.xlsx("J:/IS/BW_Balance/James Balancing/Data/GL MM.xlsx" , sheet= 1)
    PhyMM = read.xlsx("J:/IS/BW_Balance/James Balancing/Data/Physical MM.xlsx" , sheet = 1)
  }
  
  GLMM = GLMM[GLMM$Act.Cost.Cntr != "Totals" , ]
  PhyMM = PhyMM[PhyMM$Act.Cost.Cntr != "Totals" , ]
  
  names(GLMM) = gsub("\\.","",names(GLMM))
  names(PhyMM) = gsub("\\.","",names(PhyMM))
  
  names(GLMM) = gsub("\\[","",names(GLMM))
  names(PhyMM) = gsub("\\[","",names(PhyMM))
  
  names(GLMM) = gsub("\\]","",names(GLMM))
  names(PhyMM) = gsub("\\]","",names(PhyMM))
  
  names(GLMM) = gsub("\\$","",names(GLMM))
  names(PhyMM) = gsub("\\$","",names(PhyMM))
  
  #  GLMM = GLMM[,names(GLMM) != "NSISimple"]
  
  names(GLMM)[names(GLMM) == "ConaNSISimpleCona_Sales"] = "CONANSISimple"
  names(GLMM)[names(GLMM) == "NSI"] = "MMNSI"
  
  valToRet = list(GLMM,PhyMM)
  
  
  
  
  return(valToRet)
}


# Test Connections

testConnection = function(){ 

  
  KOresult = tryCatch({ dbConnect(odbc(), "senior" , uid = usr , pwd = pass)} ,
                      warning = function(w) { return("Warning") } , 
                      error = function(e) { return("Error") })
  
  

  
  BWresult = tryCatch({ RSAPConnect(ashost=Address, sysnr=Num , client=Num, user = usr , passwd = pass, lang="EN") } ,
                      warning = function(w) { return("Warning") } , 
                      error = function(e) { return("Error") })
  

  
  HANAresult = tryCatch({ dbConnect(odbc(), "HANA" , uid = usr , pwd = pass) } ,
                        warning = function(w) { return("Warning") } , 
                        error = function(e) { return("Error") })
  
  worked = class(KOresult) == "character" |  class(BWresult) == "character" | class(HANAresult) == "character"
  
  return( list( worked , KOresult , BWresult , HANAresult ) )
  
  
}


# Merge all

mergeallOld = function(HANA,KOEXTT,BWVolume,GLMM,PhyMM,ZR,YEAR,PERIOD,theDATE,END){
  
  test1 = merge(HANA[ , c("ProfitCenterCode","ProfitCenterName","Volume")] ,
                KOEXTT[ , c("PLNT","VOLUMEPHYSICAL")] ,
                by.x = "ProfitCenterCode"  ,
                by.y = "PLNT" ,
                all = T)
  names(test1) = c("CODE","COSTCENTER","HANAVOLUME","KOEXTTVOLUME")
  # test1$KOEXTTVAR = test1$HANAVOLUME - test1$KOEXTTVOLUME
  
  test1$KOEXTTVAR = ifelse(is.na(test1$HANAVOLUME) | test1$HANAVOLUME == 0 , test1$KOEXTTVOLUME - test1$KOEXTTVOLUME , test1$HANAVOLUME - test1$KOEXTTVOLUME )
  
  
  test2 = merge(test1 , 
                PhyMM[ ,c("ActCostCntrhostcode","VolGrossSalesVolume")] , 
                by.x = "CODE",
                by.y = "ActCostCntrhostcode" ,
                all = T)
  names(test2) = c("CODE","COSTCENTER","HANAVOLUME","KOEXTTVOLUME","KOEXTTVAR","MMVOLUME")
  # test2$MMVAR = test2$HANAVOLUME - test2$MMVOLUME
  
  test2$MMVAR = ifelse(is.na(test2$HANAVOLUME) | test2$HANAVOLUME == 0 , test2$KOEXTTVOLUME - test2$MMVOLUME , test2$HANAVOLUME - test2$MMVOLUME )
  
  
  test3 = merge(test2,
                ZR[,c("PLNT","ZRVOLUME")],
                by.x = "CODE",
                by.y = "PLNT" , all = T)
  # test3$ZRVAR = test3$HANAVOLUME - test3$ZRVOLUME
  
  test3$ZRVAR = ifelse(is.na(test3$HANAVOLUME) | test3$HANAVOLUME == 0 , test3$KOEXTTVOLUME - test3$ZRVOLUME , test3$HANAVOLUME - test3$ZRVOLUME )
  
  
  
  test3[,c("ZRVOLUME","ZRVAR")][is.na(test3[,c("ZRVOLUME","ZRVAR")] )] = 0
  
  # test3 = test3[!is.na(test2$COSTCENTER) , ]
  # test3[is.na(test3)] = 0
  
  VolumePhysical = test3
  VolumePhysical$DATE = as.numeric( as.Date( theDATE ) )
  VolumePhysical$PERIOD = PERIOD
  VolumePhysical$YEAR = YEAR
  VolumePhysical$PERIODEND = END
  
  
  
  
  # GL Unit
  
  test1 = merge(GLMM[,c("ActCostCntrhostcode","ActCostCntr","TotalGrossVol")],
                KOEXTT[,c("PLNT","VOLUMEGLUNIT")],
                by.x = "ActCostCntrhostcode" ,
                by.y = "PLNT" ,
                all = T)
  names(test1) = c("CODE","COSTCENTER","MMVOLUME","KOEXTTVOLUME")
  test1$MMVAR = test1$KOEXTTVOLUME - test1$MMVOLUME
  
  
  
  test2 = merge(test1 ,
                BWVolume[ , c("CostCenter","Volume")] ,
                by.x = "CODE" ,
                by.y = "CostCenter" ,
                all = T)
  test2$BWVAR = test2$KOEXTTVOLUME + test2$Volume
  names(test2) = c("CODE","COSTCENTER","MMVOLUME","VOLUMEGLUNIT","MMVAR","BWVOLUME","BWVAR")
  VolumeGL = test2[ ,c("CODE","COSTCENTER","VOLUMEGLUNIT","BWVOLUME","BWVAR","MMVOLUME","MMVAR")]
  
  test3 = merge(test2,
                ZR[,c("PLNT","ZRGLVOLUME")],
                by.x = "CODE",
                by.y = "PLNT",all=T)
  test3$ZRVAR = test3$VOLUMEGLUNIT - test3$ZRGLVOLUME
  
  test3[,c("ZRGLVOLUME","ZRVAR")][is.na(test3[,c("ZRGLVOLUME","ZRVAR")] )] = 0
  
  
  test3 = test3[!is.na(test3$COSTCENTER) , ]
  test3[is.na(test3)] = 0
  
  VolumeGL = test3
  
  VolumeGL$DATE = as.numeric( as.Date( theDATE ) )
  VolumeGL$PERIOD = PERIOD
  VolumeGL$YEAR = YEAR
  VolumeGL$PERIODEND = END
  
  # NSI
  
  test1 = merge(HANA[ , c("ProfitCenterCode","ProfitCenterName","NSI")],
                KOEXTT[ , c("PLNT" , "NSIAVV")],
                by.x = "ProfitCenterCode" ,
                by.y = "PLNT" ,
                all = T)
  names(test1) = c("CODE" , "COSTCENTER" , "HANANSI" , "KOEXTTNSIAVV" )
  # test1$KOEXTTAVVVAR = test1$HANANSI - test1$KOEXTTNSIAVV
  test1$KOEXTTAVVVAR = ifelse(is.na(test1$HANANSI) | test1$HANANSI == 0 , test1$KOEXTTNSIAVV - test1$KOEXTTNSIAVV , test1$HANANSI - test1$KOEXTTNSIAVV )
  
  
  test2 = merge(test1,
                KOEXTT[ , c("PLNT" , "NSIRSEXAM")],
                by.x = "CODE" ,
                by.y = "PLNT" ,
                all = T)
  names(test2) = c("CODE" , "COSTCENTER" , "HANANSI" , "KOEXTTAVV" ,"KOEXTTAVVVAR","KOEXTTRSEXAM")
  # test2$KOEXTTRSEXAMVAR = test2$HANANSI - test2$KOEXTTRSEXAM
  test2$KOEXTTRSEXAMVAR = ifelse(is.na(test2$HANANSI) | test2$HANANSI == 0 , test2$KOEXTTRSEXAM - test2$KOEXTTRSEXAM , test2$HANANSI - test2$KOEXTTRSEXAM )
  
  
  test3 = merge(test2,
                GLMM[,c("ActCostCntrhostcode","NSISimple","CONANSISimple","MMNSI")],
                by.x = "CODE" ,
                by.y = "ActCostCntrhostcode" ,
                all = T)
  
  # test3$MMSimpleVAR = test3$HANANSI - test3$NSISimple
  # test3$MMCONAVAR = test3$HANANSI - test3$CONANSISimple
  # test3$MMNSIVAR = test3$HANANSI - test3$MMNSI
  
  test3$MMSimpleVAR = ifelse(is.na(test3$HANANSI) | test3$HANANSI == 0 , test3$KOEXTTRSEXAM - test3$NSISimple , test3$HANANSI - test3$NSISimple )
  test3$CONANSISimpleVAR = ifelse(is.na(test3$HANANSI) | test3$HANANSI == 0 , test3$KOEXTTRSEXAM - test3$CONANSISimple , test3$HANANSI - test3$CONANSISimple )
  test3$MMNSIVAR = ifelse(is.na(test3$HANANSI) | test3$HANANSI == 0 , test3$KOEXTTRSEXAM - test3$MMNSI , test3$HANANSI - test3$MMNSI)
  
  test4 = merge(test3,
                BWVolume[,c("CostCenter","NSI")],
                by.x = "CODE" ,
                by.y = "CostCenter" ,
                all = T)
  # test4$BWVAR = test4$HANANSI + test4$NSI
  test4$BWVAR = ifelse(is.na(test4$HANANSI) | test4$HANANSI == 0 , test4$KOEXTTRSEXAM + test4$NSI , test4$HANANSI + test4$NSI)
  
  
  names(test4) = c("CODE","COSTCENTER","HANANSI","KOEXTTAVV","KOEXTTAVVVAR","KOEXTTRSEXAM","KOEXTTRSEXAMVAR","NSISimple",      
                   "CONANSISimple","MMNSI","MMSimpleVAR","MMCONAVAR","MMNSIVAR","BWNSI","BWVAR" )
  
  
  
  test5 = merge(test4,
                ZR[,c("PLNT","ZRNSI")],
                by.x = "CODE",
                by.y = "PLNT",all=T)
  # test5$ZRVAR = test5$HANANSI - test5$ZRNSI
  test5$ZRVAR = ifelse(is.na(test5$HANANSI) | test5$HANANSI == 0 , test5$KOEXTTRSEXAM - test5$ZRNSI , test5$HANANSI - test5$ZRNSI)
  
  test5[,c("ZRNSI","ZRVAR")][is.na(test5[,c("ZRNSI","ZRVAR")] )] = 0
  
  
  # test5 = test5[!is.na(test5$COSTCENTER) , ]
  test5[is.na(test5)] = 0
  
  NSI = test5
  
  NSI$DATE = as.numeric( as.Date( theDATE ) )
  NSI$PERIOD = PERIOD
  NSI$YEAR = YEAR
  NSI$PERIODEND = END
  
  # COGS
  
  test1 = merge(HANA[ , c("ProfitCenterCode","ProfitCenterName","COGS")],
                KOEXTT[ , c("PLNT" , "COGS")],
                by.x = "ProfitCenterCode" ,
                by.y = "PLNT" ,
                all = T)
  names(test1) = c("CODE" , "COSTCENTER" , "HANACOGS" , "KOEXTTCOGS" )
  # test1$KOEXTTVAR = test1$HANACOGS - test1$KOEXTTCOGS
  test1$KOEXTTVAR = ifelse(is.na(test1$HANACOGS) | test1$HANACOGS == 0 , test1$KOEXTTCOGS- test1$KOEXTTCOGS , test1$HANACOGS - test1$KOEXTTCOGS )
  
  test2 = merge(test1,
                GLMM[,c("ActCostCntrhostcode","COGSSimple")],
                by.x = "CODE" ,
                by.y = "ActCostCntrhostcode" ,
                all = T)
  # test2$MMVAR = test2$HANACOGS - test2$COGSSimple
  test2$MMVAR = ifelse(is.na(test2$HANACOGS) | test2$HANACOGS == 0 , test2$KOEXTTCOGS - test2$COGSSimple , test2$HANACOGS - test2$COGSSimple )
  
  
  names(test2) = c("CODE","COSTCENTER","HANACOGS","KOEXTTCOGS","KOEXTTVAR","MMCOGS","MMVAR" )
  
  test3 = merge(test2,
                BWVolume[,c("CostCenter","COGS")],
                by.x = "CODE" ,
                by.y = "CostCenter" ,
                all = T)
  # test3$BWVAR = test3$HANACOGS - test3$COGS
  test3$BWVAR = ifelse(is.na(test3$HANACOGS) | test3$HANACOGS == 0 , test3$KOEXTTCOGS - test3$COGS , test3$HANACOGS - test3$COGS )
  
  
  
  names(test3) = c("CODE","COSTCENTER","HANACOGS","KOEXTTCOGS","KOEXTTVAR","MMCOGS","MMVAR","BWCOGS","BWVAR")
  
  test3 = test3[,c("COSTCENTER","CODE","HANACOGS","KOEXTTCOGS","KOEXTTVAR","MMCOGS","MMVAR","BWCOGS","BWVAR")]
  
  
  test4 = merge(test3,
                ZR[,c("PLNT","ZRCOGS")],
                by.x = "CODE",
                by.y = "PLNT",all = T)
  # test4$ZRVAR = test4$HANACOGS - test4$ZRCOGS
  test4$ZRVAR = ifelse(is.na(test4$HANACOGS) | test4$HANACOGS == 0 , test4$KOEXTTCOGS - test4$ZRCOGS , test4$HANACOGS - test4$ZRCOGS )
  
  
  test4[,c("ZRCOGS","ZRVAR")][is.na(test4[,c("ZRCOGS","ZRVAR")] )] = 0
  
  # test4 = test4[!is.na(test4$COSTCENTER) , ]
  test4[is.na(test4)] = 0
  
  COGS = test4
  COGS$DATE = as.numeric( as.Date( theDATE ) )
  COGS$PERIOD = PERIOD
  COGS$YEAR = YEAR
  COGS$PERIODEND = END
  
  return(list(VolumePhysical,VolumeGL,NSI,COGS))
}


mergeall = function(HANA,KOEXTT,BWVolume,GLMM,PhyMM,ZR,YEAR,PERIOD,theDATE,END){
  
  tempPERIOD = PERIOD
  if(PERIOD %in% 1:9 ) tempPERIOD = paste0("0" , PERIOD)
  
  if(as.numeric(paste0(YEAR,tempPERIOD)) > 201804){
    
    
    legacy = setdiff(KOEXTT$PLNT , HANA$ProfitCenterCode)
    
    CONA = HANA[ , c("ProfitCenterCode","ProfitCenterName")]
    CONA$System = "CONA"
    names(CONA) = c("CODE","NAME","System")
    
    legacy = GLMM[GLMM$ActCostCntrhostcode %in% legacy , c("ActCostCntrhostcode" , "ActCostCntr") ]
    if(nrow(legacy) != 0){
      legacy$System = "Legacy"
      names(legacy) = c("CODE","NAME","System")
    }
    
  }
  
  else{
    
    
    legacy = setdiff(KOEXTT$PLNT , HANA$ProfitCenterCode)
    
    CONA = HANA[HANA$Volume != 0  , c("ProfitCenterCode","ProfitCenterName")]
    CONA$System = "CONA"
    names(CONA) = c("CODE","NAME","System")
    
    legacy = GLMM[GLMM$ActCostCntrhostcode %in% legacy , c("ActCostCntrhostcode" , "ActCostCntr") ]
    if(nrow(legacy) != 0){
      legacy$System = "Legacy"
      names(legacy) = c("CODE","NAME","System")
    }
    
  }
  
  
  theIndex = rbind(CONA,legacy)
  
  
  test1 = merge(HANA[ , c("ProfitCenterCode","ProfitCenterName","Volume")] ,
                KOEXTT[ , c("PLNT","VOLUMEPHYSICAL")] ,
                by.x = "ProfitCenterCode"  ,
                by.y = "PLNT" ,
                all = T)
  names(test1) = c("CODE","COSTCENTER","HANAVOLUME","KOEXTTVOLUME")
  # test1$KOEXTTVAR = test1$HANAVOLUME - test1$KOEXTTVOLUME
  
  test1$KOEXTTVOLUME[is.na(test1$KOEXTTVOLUME)] = 0
  
  test1$KOEXTTVAR = ifelse(is.na(test1$HANAVOLUME) | test1$HANAVOLUME == 0 , test1$KOEXTTVOLUME - test1$KOEXTTVOLUME , test1$HANAVOLUME - test1$KOEXTTVOLUME )
  
  
  test2 = merge(test1 , 
                PhyMM[ ,c("ActCostCntrhostcode","VolGrossSalesVolume")] , 
                by.x = "CODE",
                by.y = "ActCostCntrhostcode" ,
                all = T)
  names(test2) = c("CODE","COSTCENTER","HANAVOLUME","KOEXTTVOLUME","KOEXTTVAR","MMVOLUME")
  # test2$MMVAR = test2$HANAVOLUME - test2$MMVOLUME
  
  test2$MMVOLUME[is.na(test2$MMVOLUME)] = 0
  
  test2$MMVAR = ifelse(is.na(test2$HANAVOLUME) | test2$HANAVOLUME == 0 , test2$KOEXTTVOLUME - test2$MMVOLUME , test2$HANAVOLUME - test2$MMVOLUME )
  
  
  test3 = merge(test2,
                ZR[,c("PLNT","ZRVOLUME")],
                by.x = "CODE",
                by.y = "PLNT" , all = T)
  # test3$ZRVAR = test3$HANAVOLUME - test3$ZRVOLUME
  
  test3$ZRVAR = ifelse(is.na(test3$HANAVOLUME) | test3$HANAVOLUME == 0 , test3$KOEXTTVOLUME - test3$ZRVOLUME , test3$HANAVOLUME - test3$ZRVOLUME )
  
  
  
  test3[,c("ZRVOLUME","ZRVAR")][is.na(test3[,c("ZRVOLUME","ZRVAR")] )] = 0
  
  # test3 = test3[!is.na(test2$COSTCENTER) , ]
  # test3[is.na(test3)] = 0
  
  VolumePhysical = test3
  VolumePhysical$DATE = as.numeric( as.Date( theDATE ) )
  VolumePhysical$PERIOD = PERIOD
  VolumePhysical$YEAR = YEAR
  VolumePhysical$PERIODEND = END
  
  
  
  
  # GL Unit
  
  test1 = merge(GLMM[,c("ActCostCntrhostcode","ActCostCntr","TotalGrossVol")],
                KOEXTT[,c("PLNT","VOLUMEGLUNIT")],
                by.x = "ActCostCntrhostcode" ,
                by.y = "PLNT" ,
                all = T)
  names(test1) = c("CODE","COSTCENTER","MMVOLUME","KOEXTTVOLUME")
  test1$MMVAR = test1$KOEXTTVOLUME - test1$MMVOLUME
  
  
  
  test2 = merge(test1 ,
                BWVolume[ , c("CostCenter","Volume")] ,
                by.x = "CODE" ,
                by.y = "CostCenter" ,
                all = T)
  test2$BWVAR = test2$KOEXTTVOLUME + test2$Volume
  names(test2) = c("CODE","COSTCENTER","MMVOLUME","VOLUMEGLUNIT","MMVAR","BWVOLUME","BWVAR")
  VolumeGL = test2[ ,c("CODE","COSTCENTER","VOLUMEGLUNIT","BWVOLUME","BWVAR","MMVOLUME","MMVAR")]
  
  test3 = merge(test2,
                ZR[,c("PLNT","ZRGLVOLUME")],
                by.x = "CODE",
                by.y = "PLNT",all=T)
  test3$ZRVAR = test3$VOLUMEGLUNIT - test3$ZRGLVOLUME
  
  test3[,c("ZRGLVOLUME","ZRVAR")][is.na(test3[,c("ZRGLVOLUME","ZRVAR")] )] = 0
  
  
  test3 = test3[!is.na(test3$COSTCENTER) , ]
  test3[is.na(test3)] = 0
  
  VolumeGL = test3
  
  VolumeGL$DATE = as.numeric( as.Date( theDATE ) )
  VolumeGL$PERIOD = PERIOD
  VolumeGL$YEAR = YEAR
  VolumeGL$PERIODEND = END
  
  # NSI
  
  test1 = merge(HANA[ , c("ProfitCenterCode","ProfitCenterName","NSI")],
                KOEXTT[ , c("PLNT" , "NSIAVV")],
                by.x = "ProfitCenterCode" ,
                by.y = "PLNT" ,
                all = T)
  names(test1) = c("CODE" , "COSTCENTER" , "HANANSI" , "KOEXTTNSIAVV" )
  # test1$KOEXTTAVVVAR = test1$HANANSI - test1$KOEXTTNSIAVV
  
  test1$KOEXTTNSIAVV[is.na(test1$KOEXTTNSIAVV)] = 0
  test1$KOEXTTAVVVAR = ifelse(is.na(test1$HANANSI) | test1$HANANSI == 0 , test1$KOEXTTNSIAVV - test1$KOEXTTNSIAVV , test1$HANANSI - test1$KOEXTTNSIAVV )
  
  
  test2 = merge(test1,
                KOEXTT[ , c("PLNT" , "NSIRSEXAM")],
                by.x = "CODE" ,
                by.y = "PLNT" ,
                all = T)
  names(test2) = c("CODE" , "COSTCENTER" , "HANANSI" , "KOEXTTAVV" ,"KOEXTTAVVVAR","KOEXTTRSEXAM")
  # test2$KOEXTTRSEXAMVAR = test2$HANANSI - test2$KOEXTTRSEXAM
  test2$KOEXTTRSEXAMVAR = ifelse(is.na(test2$HANANSI) | test2$HANANSI == 0 , test2$KOEXTTRSEXAM - test2$KOEXTTRSEXAM , test2$HANANSI - test2$KOEXTTRSEXAM )
  
  
  test3 = merge(test2,
                GLMM[,c("ActCostCntrhostcode","NSISimple","CONANSISimple","MMNSI")],
                by.x = "CODE" ,
                by.y = "ActCostCntrhostcode" ,
                all = T)
  
  # test3$MMSimpleVAR = test3$HANANSI - test3$NSISimple
  # test3$MMCONAVAR = test3$HANANSI - test3$CONANSISimple
  # test3$MMNSIVAR = test3$HANANSI - test3$MMNSI
  
  test3$NSISimple[is.na(test3$NSISimple)] = 0
  test3$CONANSISimple[is.na(test3$CONANSISimple)] = 0
  test3$MMNSI[is.na(test3$MMNSI)] = 0
  
  
  test3$MMSimpleVAR = ifelse(is.na(test3$HANANSI) | test3$HANANSI == 0 , test3$KOEXTTRSEXAM - test3$NSISimple , test3$HANANSI - test3$NSISimple )
  test3$CONANSISimpleVAR = ifelse(is.na(test3$HANANSI) | test3$HANANSI == 0 , test3$KOEXTTRSEXAM - test3$CONANSISimple , test3$HANANSI - test3$CONANSISimple )
  test3$MMNSIVAR = ifelse(is.na(test3$HANANSI) | test3$HANANSI == 0 , test3$KOEXTTRSEXAM - test3$MMNSI , test3$HANANSI - test3$MMNSI)
  
  test4 = merge(test3,
                BWVolume[,c("CostCenter","NSI")],
                by.x = "CODE" ,
                by.y = "CostCenter" ,
                all = T)
  # test4$BWVAR = test4$HANANSI + test4$NSI
  test4$BWVAR = ifelse(is.na(test4$HANANSI) | test4$HANANSI == 0 , test4$KOEXTTRSEXAM + test4$NSI , test4$HANANSI + test4$NSI)
  
  
  names(test4) = c("CODE","COSTCENTER","HANANSI","KOEXTTAVV","KOEXTTAVVVAR","KOEXTTRSEXAM","KOEXTTRSEXAMVAR","NSISimple",      
                   "CONANSISimple","MMNSI","MMSimpleVAR","MMCONAVAR","MMNSIVAR","BWNSI","BWVAR" )
  
  
  
  test5 = merge(test4,
                ZR[,c("PLNT","ZRNSI")],
                by.x = "CODE",
                by.y = "PLNT",all=T)
  # test5$ZRVAR = test5$HANANSI - test5$ZRNSI
  test5$ZRVAR = ifelse(is.na(test5$HANANSI) | test5$HANANSI == 0 , test5$KOEXTTRSEXAM - test5$ZRNSI , test5$HANANSI - test5$ZRNSI)
  
  test5[,c("ZRNSI","ZRVAR")][is.na(test5[,c("ZRNSI","ZRVAR")] )] = 0
  
  
  # test5 = test5[!is.na(test5$COSTCENTER) , ]
  test5[is.na(test5)] = 0
  
  NSI = test5
  
  NSI$DATE = as.numeric( as.Date( theDATE ) )
  NSI$PERIOD = PERIOD
  NSI$YEAR = YEAR
  NSI$PERIODEND = END
  
  # COGS
  
  test1 = merge(HANA[ , c("ProfitCenterCode","ProfitCenterName","COGS")],
                KOEXTT[ , c("PLNT" , "COGS")],
                by.x = "ProfitCenterCode" ,
                by.y = "PLNT" ,
                all = T)
  names(test1) = c("CODE" , "COSTCENTER" , "HANACOGS" , "KOEXTTCOGS" )
  # test1$KOEXTTVAR = test1$HANACOGS - test1$KOEXTTCOGS
  test1$KOEXTTCOGS[is.na(test1$KOEXTTCOGS)]=0
  
  test1$KOEXTTVAR = ifelse(is.na(test1$HANACOGS) | test1$HANACOGS == 0 , test1$KOEXTTCOGS- test1$KOEXTTCOGS , test1$HANACOGS - test1$KOEXTTCOGS )
  
  test2 = merge(test1,
                GLMM[,c("ActCostCntrhostcode","COGSSimple")],
                by.x = "CODE" ,
                by.y = "ActCostCntrhostcode" ,
                all = T)
  # test2$MMVAR = test2$HANACOGS - test2$COGSSimple
  
  test2$COGSSimple[is.na(test2$COGSSimple)] = 0
  
  test2$MMVAR = ifelse(is.na(test2$HANACOGS) | test2$HANACOGS == 0 , test2$KOEXTTCOGS - test2$COGSSimple , test2$HANACOGS - test2$COGSSimple )
  
  
  names(test2) = c("CODE","COSTCENTER","HANACOGS","KOEXTTCOGS","KOEXTTVAR","MMCOGS","MMVAR" )
  
  test3 = merge(test2,
                BWVolume[,c("CostCenter","COGS")],
                by.x = "CODE" ,
                by.y = "CostCenter" ,
                all = T)
  # test3$BWVAR = test3$HANACOGS - test3$COGS
  test3$BWVAR = ifelse(is.na(test3$HANACOGS) | test3$HANACOGS == 0 , test3$KOEXTTCOGS - test3$COGS , test3$HANACOGS - test3$COGS )
  
  
  
  names(test3) = c("CODE","COSTCENTER","HANACOGS","KOEXTTCOGS","KOEXTTVAR","MMCOGS","MMVAR","BWCOGS","BWVAR")
  
  test3 = test3[,c("COSTCENTER","CODE","HANACOGS","KOEXTTCOGS","KOEXTTVAR","MMCOGS","MMVAR","BWCOGS","BWVAR")]
  
  
  test4 = merge(test3,
                ZR[,c("PLNT","ZRCOGS")],
                by.x = "CODE",
                by.y = "PLNT",all = T)
  # test4$ZRVAR = test4$HANACOGS - test4$ZRCOGS
  test4$ZRVAR = ifelse(is.na(test4$HANACOGS) | test4$HANACOGS == 0 , test4$KOEXTTCOGS - test4$ZRCOGS , test4$HANACOGS - test4$ZRCOGS )
  
  
  test4[,c("ZRCOGS","ZRVAR")][is.na(test4[,c("ZRCOGS","ZRVAR")] )] = 0
  
  # test4 = test4[!is.na(test4$COSTCENTER) , ]
  test4[is.na(test4)] = 0
  
  COGS = test4
  COGS$DATE = as.numeric( as.Date( theDATE ) )
  COGS$PERIOD = PERIOD
  COGS$YEAR = YEAR
  COGS$PERIODEND = END
  
  
  VolumePhysical = merge(theIndex , VolumePhysical , by = "CODE")
  VolumePhysical = VolumePhysical[ , c("CODE","NAME","HANAVOLUME","KOEXTTVOLUME","KOEXTTVAR","MMVOLUME","MMVAR","ZRVOLUME","ZRVAR","DATE","PERIOD","YEAR","PERIODEND","System")]
  names(VolumePhysical)[2] = "COSTCENTER"
  
  VolumeGL = merge(theIndex , VolumeGL , by = "CODE")
  VolumeGL = VolumeGL[,c("CODE","NAME","MMVOLUME","VOLUMEGLUNIT","MMVAR","BWVOLUME","BWVAR","ZRGLVOLUME","ZRVAR","DATE","PERIOD","YEAR","PERIODEND","System")]
  names(VolumeGL)[2] = "COSTCENTER"
  
  NSI = merge(theIndex , NSI , by = "CODE")
  NSI = NSI[,c("CODE","NAME","HANANSI","KOEXTTAVV","KOEXTTAVVVAR","KOEXTTRSEXAM","KOEXTTRSEXAMVAR","NSISimple","CONANSISimple","MMNSI"          ,
               "MMSimpleVAR","MMCONAVAR","MMNSIVAR","BWNSI","BWVAR","ZRNSI","ZRVAR","DATE","PERIOD","YEAR","PERIODEND","System")]
  names(NSI)[2] = "COSTCENTER"
  
  COGS = merge(theIndex , COGS , by = "CODE")
  COGS = COGS[,c("CODE","NAME","HANACOGS","KOEXTTCOGS","KOEXTTVAR","MMCOGS","MMVAR","BWCOGS","BWVAR","ZRCOGS","ZRVAR","DATE","PERIOD","YEAR","PERIODEND","System")]
  names(COGS)[2] = "COSTCENTER"
  
  return(list(VolumePhysical,VolumeGL,NSI,COGS))
}

# Reading Writing 

readwrite = function(VolumePhysical,VolumeGL,NSI,COGS,YEAR,PERIOD,theDATE) {
  
  INFO = data.frame(YEAR , PERIOD , TIME = PEtheDATE )
  
  # Volume
  
  VolumePhysical$MostRecent = NA
  VolumeGL$MostRecent = NA
  NSI$MostRecent = NA
  COGS$MostRecent = NA
  
  fileName = "C:/Users/jamhill/Desktop/Balancing R/ALLDATA UPDATE.xlsx"
  
  PV = unique( openxlsx::read.xlsx(fileName , sheet = 1 ) )
  GLUV = unique( openxlsx::read.xlsx(fileName , sheet = 2 ) )
  N = unique( openxlsx::read.xlsx(fileName , sheet = 3 ) )
  C = unique( openxlsx::read.xlsx(fileName , sheet = 4 ) )
  
  
  PV$DATE = as.numeric(as.Date(PV$DATE,origin = "1900-01-01")-2)
  GLUV$DATE = as.numeric(as.Date(GLUV$DATE,origin = "1900-01-01")-2)
  N$DATE = as.numeric(as.Date(N$DATE,origin = "1900-01-01")-2)
  C$DATE = as.numeric(as.Date(C$DATE,origin = "1900-01-01")-2)
  
  PV = unique( rbind( PV , VolumePhysical ) )
  GLUV = unique( rbind( GLUV , VolumeGL ) )
  N = unique( rbind( N  , NSI ) )
  C = unique( rbind( C  , COGS ) )
  
  PV$DATE = as.Date(PV$DATE,origin = "1970-01-01")
  GLUV$DATE = as.Date(GLUV$DATE,origin = "1970-01-01")
  N$DATE = as.Date(N$DATE,origin = "1970-01-01")
  C$DATE = as.Date(C$DATE,origin = "1970-01-01")
  
  PV = mostRecent(PV)
  GLUV = mostRecent(GLUV)
  N = mostRecent(N)
  C = mostRecent(C)
  
  PV = unique(PV)
  GLUV = unique(GLUV)
  N = unique(N)
  C = unique(C)
  
  
  
  xlsx::write.xlsx(PV , fileName , sheetName="Physical Volume" , row.names = F , append = F)
  xlsx::write.xlsx(GLUV , fileName , sheetName="GL Unit Volume" , row.names = F , append = T)
  xlsx::write.xlsx(N , fileName , sheetName="NSI" , row.names = F , append = T)
  xlsx::write.xlsx(C , fileName , sheetName="COGS" , row.names = F , append = T)
  xlsx::write.xlsx(INFO , fileName , sheetName="INFO" , row.names = F , append = T)
  
  
  
  
}


# Open EXCEL


excel = function() {
  
  shell.exec( "C:/Users/jamhill/Desktop/Balancing R/close.xlsm" )
  Sys.sleep(30)
  
  shell.exec( "C:/Users/jamhill/Desktop/Balancing R/ALLDATA UPDATE.xlsx" )
  Sys.sleep(30)
  shell.exec( "C:/Users/jamhill/Desktop/Balancing R/Balancing Workbooks/BALANCING 2018.xlsx" )
  Sys.sleep(30)
  
  
  shell.exec( "C:/Users/jamhill/Desktop/Balancing R/NEW MACRO FORMATING.xlsm" )
  
  
}



# SQL Functions

writetoPhysical = function(FULL,theTime){
  
  names(FULL) = toupper(names(FULL))
  
  PERIOD = unique(FULL$PERIOD)
  YEAR = unique(FULL$YEAR)
  

  
  conKO <- dbConnect(odbc(), "senior",
                     uid = usr , 
                     pwd = pass)
  
  FULL[is.na(FULL)] = 0
  FULL = FULL[FULL$CODE != "   " , ]
  
  temp = dbGetQuery(conKO , "SELECT * FROM SWIRE.BALANCINGPHYSICAL" )
  
  for( i in 1:ncol(FULL)){if(class(FULL[,i]) %in% c("numeric","array")) FULL[,i] = round(FULL[,i],digits = 2)}
  for( i in 1:ncol(temp)){if(class(temp[,i]) %in% c("numeric","array")) temp[,i] = round(temp[,i],digits = 2)}
  
  if(( nrow(unique(temp[,-c(14,15,16)])) == nrow(unique(rbind(temp[,-c(14,15,16)],FULL))) ) == F ) {
    
    FULL$MOSTRECENT = "YES"
    FULL$DATETIME = Sys.time()
    
    SCRIPT = paste0("SELECT MAX(TIMESRANTODAY) FROM SWIRE.BALANCINGPHYSICAL WHERE PERIOD = ",unique(FULL$PERIOD) , " AND DATE = ", unique(FULL$DATE) )
    
    if( is.na( dbGetQuery(conKO , SCRIPT) ) ) FULL$TIMESRANTODAY = 1 
    if( !is.na( dbGetQuery(conKO , SCRIPT) ) ) FULL$TIMESRANTODAY = as.numeric(1 + dbGetQuery(conKO , SCRIPT) )
    
    
    
    
    
    FULL = FULL[!(is.na(FULL$COSTCENTER) | is.na(FULL$HANAVOLUME) | is.na(FULL$KOEXTTVOLUME)) , ]
    
    SCRIPT = paste0("UPDATE SWIRE.BALANCINGPHYSICAL set MOSTRECENT = \'NO\' 
                    WHERE PERIOD = " , PERIOD , " AND YEAR = " , YEAR)
    dbGetQuery(conKO , SCRIPT )
    
    
    
    for(i in 1:nrow(FULL)){
      
      KVP = FULL[i,]
      
      
      if(nrow(KVP) == 0 ) next
      
      #  KVP$DATETIME = Sys.time()
      # KVP$TIMESRANTODAY = 1 
      
      
      SCRIPT = paste(
        paste(
          paste("SELECT" , KVP[,1] ) ,
          paste0("\'" , trimws(KVP[,2]) , "\'") ,
          KVP[,3],
          KVP[,4],
          KVP[,5],
          KVP[,6],
          KVP[,7],
          KVP[,8],
          KVP[,9],
          paste0("\'" , trimws(KVP[,10]) , "\'") ,
          KVP[,11],
          KVP[,12],
          paste0("\'" , trimws(KVP[,13]) , "\'") ,
          paste0("\'" , trimws(KVP[,14]) , "\'") ,
          paste0("\'" , trimws(KVP[,15]) , "\'") ,
          paste0("\'" , trimws(KVP[,16]) , "\'") ,
          KVP[,17],
          sep = ",") ,
        "FROM SYSIBM.SYSDUMMY1 UNION ALL \n")
      
      SCRIPT[length(SCRIPT)] = gsub("UNION ALL","",SCRIPT[length(SCRIPT)])
      SCRIPT = paste(SCRIPT,collapse="")
      SCRIPT = paste("INSERT INTO SWIRE.BALANCINGPHYSICAL(CODE,COSTCENTER,HANAVOLUME,KOEXTTVOLUME,KOEXTTVAR,MMVOLUME,MMVAR,ZRVOLUME,ZRVAR,DATE,PERIOD,YEAR,PERIODEND,System,MOSTRECENT,DATETIME,TIMESRANTODAY)",SCRIPT )
      
      dbGetQuery(conKO , SCRIPT )
      
      
    }
    
    
  }
  
  
  
  SCRIPT = paste0("UPDATE SWIRE.BALANCINGPHYSICAL set DATETIME = \'",theTime,"\'")
  
  dbGetQuery(conKO , SCRIPT )
  
  
  
  dbDisconnect(conKO)
  
  
}

writetoGL = function(FULL,theTime){
  
  names(FULL) = toupper(names(FULL))
  
  PERIOD = unique(FULL$PERIOD)
  YEAR = unique(FULL$YEAR)
  

  
  conKO <- dbConnect(odbc(), "senior",
                     uid = usr , 
                     pwd = pass)
  
  FULL[is.na(FULL)] = 0
  FULL = FULL[FULL$CODE != "   " , ]
  
  temp = dbGetQuery(conKO , "SELECT * FROM SWIRE.BALANCINGGL" )
  
  for( i in 1:ncol(FULL)){if(class(FULL[,i]) %in% c("numeric","array") ) FULL[,i] = round(FULL[,i],digits = 2)}
  for( i in 1:ncol(temp)){if(class(temp[,i]) %in% c("numeric","array") ) temp[,i] = round(temp[,i],digits = 2)}
  
  if(( nrow(unique(temp[,-c(14,15,16)])) == nrow(unique(rbind(temp[,-c(14,15,16)],FULL) )) ) == F ) {
    
    FULL$MOSTRECENT = "YES"
    FULL$DATETIME = Sys.time()
    
    SCRIPT = paste0("SELECT MAX(TIMESRANTODAY) FROM SWIRE.BALANCINGGL WHERE PERIOD = ",unique(FULL$PERIOD) , " AND DATE = ", unique(FULL$DATE) )
    
    if( is.na( dbGetQuery(conKO , SCRIPT) ) ) FULL$TIMESRANTODAY = 1 
    if( !is.na( dbGetQuery(conKO , SCRIPT) ) ) FULL$TIMESRANTODAY = as.numeric(1 + dbGetQuery(conKO , SCRIPT) )
    
    
    
    
    
    FULL = FULL[!(is.na(FULL$COSTCENTER) | is.na(FULL$VOLUMEGLUNIT) | is.na(FULL$BWVOLUME)) , ]
    
    SCRIPT = paste0("UPDATE SWIRE.BALANCINGGL set MOSTRECENT = \'NO\' 
                    WHERE PERIOD = " , PERIOD , " AND YEAR = " , YEAR)
    dbGetQuery(conKO , SCRIPT )
    
    
    
    FULL = mostRecent(FULL)
    
    for(i in 1:nrow(FULL)){
      
      KVP = FULL[i,]
      
      # KVP$DATETIME = Sys.time()
      # KVP$TIMESRANTODAY = 1 
      
      
      SCRIPT = paste(
        paste(
          paste("SELECT" , KVP[,1] ) ,
          paste0("\'" , trimws(KVP[,2]) , "\'") ,
          KVP[,3],
          KVP[,4],
          KVP[,5],
          KVP[,6],
          KVP[,7],
          KVP[,8],
          KVP[,9],
          paste0("\'" , trimws(KVP[,10]) , "\'") ,
          KVP[,11],
          KVP[,12],
          paste0("\'" , trimws(KVP[,13]) , "\'") ,
          paste0("\'" , trimws(KVP[,14]) , "\'") ,
          paste0("\'" , trimws(KVP[,15]) , "\'") ,
          paste0("\'" , trimws(KVP[,16]) , "\'") ,
          KVP[,17],
          sep = ",") ,
        "FROM SYSIBM.SYSDUMMY1 UNION ALL \n")
      
      SCRIPT[length(SCRIPT)] = gsub("UNION ALL","",SCRIPT[length(SCRIPT)])
      SCRIPT = paste(SCRIPT,collapse="")
      SCRIPT = paste("INSERT INTO SWIRE.BALANCINGGL(CODE,COSTCENTER,MMVOLUME,VOLUMEGLUNIT,MMVAR,BWVOLUME,BWVAR,ZRGLVOLUME,ZRVAR,DATE,PERIOD,YEAR,PERIODEND,SYSTEM,MOSTRECENT,DATETIME,TIMESRANTODAY)",SCRIPT )
      
      dbGetQuery(conKO , SCRIPT )
      
      
    }
    
  }
  
  SCRIPT = paste0("UPDATE SWIRE.BALANCINGGL set DATETIME = \'",theTime,"\'")
  
  dbGetQuery(conKO , SCRIPT )
  
  
  
  dbDisconnect(conKO)
  
  
}

writetoNSI = function(FULL,theTime){
  
  names(FULL) = toupper(names(FULL))
  
  PERIOD = unique(FULL$PERIOD)
  YEAR = unique(FULL$YEAR)
  

  
  conKO <- dbConnect(odbc(), "senior",
                     uid = usr , 
                     pwd = pass)
  
  FULL[is.na(FULL)] = 0
  FULL = FULL[FULL$CODE != "   " , ]
  
  temp = dbGetQuery(conKO , "SELECT * FROM SWIRE.BALANCINGNSI" )
  
  for( i in 1:ncol(FULL)){if(class(FULL[,i]) %in% c("numeric","array") ) FULL[,i] = round(FULL[,i],digits = 2)}
  for( i in 1:ncol(temp)){if(class(temp[,i]) %in% c("numeric","array") ) temp[,i] = round(temp[,i],digits = 2)}
  
  if(( nrow(unique(temp[,-c(22,23,24)])) == nrow(unique(rbind(temp[,-c(22,23,24)],FULL) )) ) == F ) {
    
    FULL$MOSTRECENT = "YES"
    FULL$DATETIME = Sys.time()
    
    SCRIPT = paste0("SELECT MAX(TIMESRANTODAY) FROM SWIRE.BALANCINGNSI WHERE PERIOD = ",unique(FULL$PERIOD) , " AND DATE = ", unique(FULL$DATE) )
    
    if( is.na( dbGetQuery(conKO , SCRIPT) ) ) FULL$TIMESRANTODAY = 1 
    if( !is.na( dbGetQuery(conKO , SCRIPT) ) ) FULL$TIMESRANTODAY = as.numeric(1 + dbGetQuery(conKO , SCRIPT) )
    
    
    
    
    
    FULL = FULL[!(is.na(FULL$HANANSI) | is.na(FULL$KOEXTTAVV) | is.na(FULL$BWNSI)) , ]
    
    SCRIPT = paste0("UPDATE SWIRE.BALANCINGNSI set MOSTRECENT = \'NO\' 
                    WHERE PERIOD = " , PERIOD , " AND YEAR = " , YEAR)
    dbGetQuery(conKO , SCRIPT )
    
    
    for(i in 1:nrow(FULL)){
      
      KVP = FULL[i,]
      
      #  KVP$DATETIME = Sys.time()
      # KVP$TIMESRANTODAY = 1 
      
      
      SCRIPT = paste(
        paste(
          paste("SELECT" , KVP[,1] ) ,
          paste0("\'" , trimws(KVP[,2]) , "\'") ,
          KVP[,3],
          KVP[,4],
          KVP[,5],
          KVP[,6],
          KVP[,7],
          KVP[,8],
          KVP[,9],
          KVP[,10],
          KVP[,11],
          KVP[,12],
          KVP[,13],
          KVP[,14],
          KVP[,15],
          KVP[,16],
          KVP[,17],
          paste0("\'" , trimws(KVP[,18]) , "\'") ,
          KVP[,19],
          KVP[,20],
          paste0("\'" , trimws(KVP[,21]) , "\'") ,
          paste0("\'" , trimws(KVP[,22]) , "\'") ,
          paste0("\'" , trimws(KVP[,23]) , "\'") ,
          paste0("\'" , trimws(KVP[,24]) , "\'") ,
          KVP[,25],
          sep = ",") ,
        "FROM SYSIBM.SYSDUMMY1 UNION ALL \n")
      
      SCRIPT[length(SCRIPT)] = gsub("UNION ALL","",SCRIPT[length(SCRIPT)])
      SCRIPT = paste(SCRIPT,collapse="")
      SCRIPT = paste("INSERT INTO SWIRE.BALANCINGNSI(CODE,COSTCENTER,HANANSI,KOEXTTAVV,KOEXTTAVVVAR,KOEXTTRSEXAM,KOEXTTRSEXAMVAR,NSISimple,CONANSISimple,MMNSI,MMSimpleVAR
                     ,MMCONAVAR,MMNSIVAR,BWNSI,BWVAR,ZRNSI,ZRVAR,DATE,PERIOD,YEAR,PERIODEND,SYSTEM,MOSTRECENT,DATETIME,TIMESRANTODAY)",SCRIPT )
      
      dbGetQuery(conKO , SCRIPT )
      
      
    }
    
  }
  
  SCRIPT = paste0("UPDATE SWIRE.BALANCINGNSI set DATETIME = \'",theTime,"\'")
  
  dbGetQuery(conKO , SCRIPT )
  
  
  
  dbDisconnect(conKO)
  
  
}

writetoCOGS = function(FULL,theTime){
  
  names(FULL) = toupper(names(FULL))
  
  PERIOD = unique(FULL$PERIOD)
  YEAR = unique(FULL$YEAR)
  

  
  conKO <- dbConnect(odbc(), "senior",
                     uid = usr , 
                     pwd = pass)
  
  FULL[is.na(FULL)] = 0
  FULL = FULL[FULL$CODE != "   " , ]
  
  temp = dbGetQuery(conKO , "SELECT * FROM SWIRE.BALANCINGCOGS" )
  
  for( i in 1:ncol(FULL)){if(class(FULL[,i]) %in% c("numeric","array")) FULL[,i] = round(FULL[,i],digits = 2)}
  for( i in 1:ncol(temp)){if(class(temp[,i]) %in% c("numeric","array")) temp[,i] = round(temp[,i],digits = 2)}
  
  if(( nrow(unique(temp[,-c(16,17,18)])) == nrow(unique(rbind(temp[,-c(16,17,18)],FULL) )) ) == F ) {
    
    FULL$MOSTRECENT = "YES"
    FULL$DATETIME = Sys.time()
    
    SCRIPT = paste0("SELECT MAX(TIMESRANTODAY) FROM SWIRE.BALANCINGCOGS WHERE PERIOD = ",unique(FULL$PERIOD) , " AND DATE = ", unique(FULL$DATE) )
    
    if( is.na( dbGetQuery(conKO , SCRIPT) ) ) FULL$TIMESRANTODAY = 1 
    if( !is.na( dbGetQuery(conKO , SCRIPT) ) ) FULL$TIMESRANTODAY = as.numeric(1 + dbGetQuery(conKO , SCRIPT) )
    
    
    
    
    
    FULL = FULL[!(is.na(FULL$COSTCENTER) | is.na(FULL$HANACOGS) | is.na(FULL$KOEXTTCOGS)) , ]
    
    SCRIPT = paste0("UPDATE SWIRE.BALANCINGCOGS set MOSTRECENT = \'NO\' 
                    WHERE PERIOD = " , PERIOD , " AND YEAR = " , YEAR)
    dbGetQuery(conKO , SCRIPT )
    
    
    
    
    FULL = mostRecent(FULL)
    
    for(i in 1:nrow(FULL)){
      
      KVP = FULL[i,]
      
      #  KVP$DATETIME = Sys.time()
      # KVP$TIMESRANTODAY = 1 
      
      
      SCRIPT = paste(
        paste(
          paste("SELECT" , KVP[,1] ) ,
          paste0("\'" , trimws(KVP[,2]) , "\'") ,
          KVP[,3],
          KVP[,4],
          KVP[,5],
          KVP[,6],
          KVP[,7],
          KVP[,8],
          KVP[,9],
          KVP[,10],
          KVP[,11],
          paste0("\'" , trimws(KVP[,12]) , "\'") ,
          KVP[,13],
          KVP[,14],
          paste0("\'" , trimws(KVP[,15]) , "\'") ,
          paste0("\'" , trimws(KVP[,16]) , "\'") ,
          paste0("\'" , trimws(KVP[,17]) , "\'") ,
          paste0("\'" , trimws(KVP[,18]) , "\'") ,
          KVP[,19],
          sep = ",") ,
        "FROM SYSIBM.SYSDUMMY1 UNION ALL \n")
      
      SCRIPT[length(SCRIPT)] = gsub("UNION ALL","",SCRIPT[length(SCRIPT)])
      SCRIPT = paste(SCRIPT,collapse="")
      SCRIPT = paste("INSERT INTO SWIRE.BALANCINGCOGS(CODE,COSTCENTER,HANACOGS,KOEXTTCOGS,KOEXTTVAR,MMCOGS,MMVAR,BWCOGS,BWVAR,ZRCOGS,ZRVAR,DATE,PERIOD,YEAR,PERIODEND,SYSTEM,MOSTRECENT,DATETIME,TIMESRANTODAY)",SCRIPT )
      
      dbGetQuery(conKO , SCRIPT )
      
    }
    
  }
  
  SCRIPT = paste0("UPDATE SWIRE.BALANCINGCOGS set DATETIME = \'",theTime,"\'")
  
  dbGetQuery(conKO , SCRIPT )
  
  
  
  dbDisconnect(conKO)
  
  
}





