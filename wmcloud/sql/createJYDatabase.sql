-- Server version
-- Require MySQL >= 5.6.5
--
-- Table structure for table `marketDay`
--

CREATE DATABASE IF NOT EXISTS `JYData` DEFAULT CHARSET=utf8;
use JYData;

DROP TABLE IF EXISTS `marketDay`;
CREATE TABLE `marketDay` (
    `ticker` varchar(63) NOT NULL,
    `tradeDate` datetime NOT NULL,
    `secID` varchar(63) DEFAULT NULL,
    `preClosePrice` double DEFAULT NULL,
    `openPrice` double DEFAULT NULL,
    `highestPrice` double DEFAULT NULL,
    `lowestPrice` double DEFAULT NULL,
    `closePrice` double DEFAULT NULL,
    `turnoverVol` double DEFAULT NULL,
    `turnovervalue` double DEFAULT NULL,
    `dealAmount` double DEFAULT NULL,

    `exchangeCD` varchar(63) DEFAULT NULL,
    `secShortName` varchar(63) DEFAULT NULL,
    `secShortNameEN` varchar(63) DEFAULT NULL,

    `createTime` datetime NOT NULL DEFAULT NOW(),
    `updateTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    /* PRIMARY KEY (`ticker`, `tradeDate`) */
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='日行情表';

DROP TABLE IF EXISTS `divident`;
CREATE TABLE `divident` (
    `ticker` bigint(20) DEFAULT NULL,
    `secID` varchar(63) DEFAULT NULL,
    `exDivDate` datetime DEFAULT NULL,
    `perCashDiv` double DEFAULT NULL,
    `perCashDivAfTax` double DEFAULT NULL,
    `perShareDivRatio` double DEFAULT NULL,
    `perShareTransRatio` double DEFAULT NULL,

    `bLastTradeDate` datetime DEFAULT NULL,
    `baseShares` double DEFAULT NULL,
    `bonusShareListDate` datetime DEFAULT NULL,
    `divObjectCD` varchar(63) DEFAULT NULL,
    `endDate` datetime DEFAULT NULL,
    `eventProcessCD` varchar(63) DEFAULT NULL,
    `exchangeCD` varchar(63) DEFAULT NULL,
    `frCurrencyCD` varchar(63) DEFAULT NULL,
    `frPerCashDiv` double DEFAULT NULL,
    `frPerCashDivAfTax` double DEFAULT NULL,
    `frTotalCashDiv` double DEFAULT NULL,
    `imPublishDate` datetime DEFAULT NULL,
    `isDiv` bigint(20) DEFAULT NULL,
    `payCashDate` datetime DEFAULT NULL,
    `planPublishDate` datetime DEFAULT NULL,
    `recordDate` datetime DEFAULT NULL,
    `secShortName` varchar(63) DEFAULT NULL,
    `secShortNameEn` varchar(63) DEFAULT NULL,
    `sharesAfDiv` double DEFAULT NULL,
    `totalCashDiv` double DEFAULT NULL,

    `createTime` datetime NOT NULL DEFAULT NOW(),
    `updateTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    /* PRIMARY KEY (`ticker`, `exDivDate`) */
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='分红信息';


DROP TABLE IF EXISTS `rightsIssue`;
CREATE TABLE `rightsIssue` (
    `ticker` bigint(20) DEFAULT NULL,
    `secID` varchar(63) DEFAULT NULL,
    `exRightsDate` datetime DEFAULT NULL,
    `allotmentPrice` double DEFAULT NULL,
    `allotmentRatio` double DEFAULT NULL,

    `allotAbbr` varchar(63) DEFAULT NULL,
    `allotCode` varchar(63) DEFAULT NULL,
    `allotCost` double DEFAULT NULL,
    `allotShares` double DEFAULT NULL,
    `allotYear` datetime DEFAULT NULL,
    `baseShares` double DEFAULT NULL,
    `equTypeCD` varchar(63) DEFAULT NULL,
    `exchangeCD` varchar(63) DEFAULT NULL,
    `iniPublishDate` datetime DEFAULT NULL,
    `listDate` datetime DEFAULT NULL,
    `payBeginDate` datetime DEFAULT NULL,
    `payEndDate` datetime DEFAULT NULL,
    `planPublishDate` datetime DEFAULT NULL,
    `proPublishDate` datetime DEFAULT NULL,
    `raiseCap` double DEFAULT NULL,
    `recordDate` datetime DEFAULT NULL,
    `secShortName` varchar(63) DEFAULT NULL,
    `secShortNameEn` varchar(63) DEFAULT NULL,

    `createTime` datetime NOT NULL DEFAULT NOW(),
    `updateTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    /* PRIMARY KEY (`ticker`, `exRightsDate`) */
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='配股信息';
