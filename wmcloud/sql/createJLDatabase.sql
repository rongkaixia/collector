-- Server version
-- Require MySQL >= 5.6.5
--
-- Table structure for table `marketDay`
--

CREATE DATABASE IF NOT EXISTS `JLData` DEFAULT CHARSET=utf8;
use JLData;

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

    `chg` double DEFAULT NULL,
    `chgPct` double DEFAULT NULL,
    `exchangeCD` varchar(63) DEFAULT NULL,
    `secShortName` varchar(63) DEFAULT NULL,

    `createTime` datetime NOT NULL DEFAULT NOW(),
    `updateTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    /* PRIMARY KEY (`ticker`, `tradeDate`) */
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='日行情表';

DROP TABLE IF EXISTS `dividend`;
CREATE TABLE `dividend` (
    `Aticker` bigint(20) DEFAULT NULL,
    `exDivDate` datetime DEFAULT NULL,
    `AsecShortName` varchar(63) DEFAULT NULL,
    `perCashDiv` double DEFAULT NULL,
    `perCashDivAfTax` double DEFAULT NULL,
    `perShareDivRatio` double DEFAULT NULL,
    `perShareTransRatio` double DEFAULT NULL,

    `BsecShortName` varchar(63) DEFAULT NULL,
    `Bticker` bigint(20) DEFAULT NULL,
    `bLastTradeDate` datetime DEFAULT NULL,
    `baseShares` double DEFAULT NULL,
    `boPublishDate` datetime DEFAULT NULL,
    `bonusShareListDate` datetime DEFAULT NULL,
    `currencyCD` varchar(63) DEFAULT NULL,
    `divObject` varchar(63) DEFAULT NULL,
    `endDate` datetime DEFAULT NULL,
    `equTypeCD` varchar(63) DEFAULT NULL,
    `eventNum` varchar(63) DEFAULT NULL,
    `eventProcessCD` varchar(63) DEFAULT NULL,
    `imPublishDate` datetime DEFAULT NULL,
    `partyID` bigint(20) DEFAULT NULL,
    `payCashDate` datetime DEFAULT NULL,
    `recordDate` datetime DEFAULT NULL,
    `transShareListDate` datetime DEFAULT NULL,

    `createTime` datetime NOT NULL DEFAULT NOW(),
    `updateTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    /* PRIMARY KEY (`ticker`, `exDivDate`) */
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='分红信息';

DROP TABLE IF EXISTS `rightsIssue`;
CREATE TABLE `rightsIssue` (
    `aticker` bigint(20) DEFAULT NULL,
    `exRightsDateA` datetime DEFAULT NULL,
    `allotmentPrice` double DEFAULT NULL,
    `allotmentRatio` double DEFAULT NULL,

    `aSecShortName` varchar(63) DEFAULT NULL,
    `allotCost` double DEFAULT NULL,
    `allotFrPrice` double DEFAULT NULL,
    `allotShares` double DEFAULT NULL,
    `bSecShortName` varchar(63) DEFAULT NULL,
    `baseShares` bigint(20) DEFAULT NULL,
    `bticker` bigint(20) DEFAULT NULL,
    `currencyCD` varchar(63) DEFAULT NULL,
    `dSeq` bigint(20) DEFAULT NULL,
    `eventNum` varchar(63) DEFAULT NULL,
    `exRightsDateB` datetime DEFAULT NULL,
    `listDate` datetime DEFAULT NULL,
    `partyID` bigint(20) DEFAULT NULL,
    `raiseCap` double DEFAULT NULL,
    `recordDateA` datetime DEFAULT NULL,
    `recordDateB` datetime DEFAULT NULL,
    `sharesAfAllot` double DEFAULT NULL,
    `sharesBfAllot` double DEFAULT NULL,

    `createTime` datetime NOT NULL DEFAULT NOW(),
    `updateTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8
