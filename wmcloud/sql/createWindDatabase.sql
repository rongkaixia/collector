-- Server version
-- Require MySQL >= 5.6.5
--
-- Table structure for table `marketDay`
--

DROP DATABASE IF EXISTS `WindData`;
CREATE DATABASE IF NOT EXISTS `WindData` DEFAULT CHARACTER SET utf8;
use WindData;

DROP TABLE IF EXISTS `dividend`;
CREATE TABLE `dividend` (
	`wind_code` varchar(63) DEFAULT NULL,
	`ex_dividend_date` datetime DEFAULT NULL,
	`sec_name` varchar(63) DEFAULT NULL,
	`cash_payout_ratio` double DEFAULT NULL,
	`stock_split_ratio` double DEFAULT NULL,
	`stock_dividend_ratio` double DEFAULT NULL,
	`seo_ratio` double DEFAULT NULL,
	`seo_price` double DEFAULT NULL,
	`rights_issue_price` double DEFAULT NULL,
	`rights_issue_ratio` double DEFAULT NULL,
	`ex_dividend_note` varchar(63) DEFAULT NULL,
  
    `createTime` datetime NOT NULL DEFAULT NOW(),
    `updateTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8