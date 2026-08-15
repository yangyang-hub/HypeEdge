-- 0002: funding_arb_config_versions 增加 max_hold_hours（M-FA7 最大持仓时限）
-- 与 FundingArbParams::validate 及 config_normalize 校验一致：BETWEEN 1 AND 8760。
ALTER TABLE funding_arb_config_versions
    ADD COLUMN max_hold_hours BIGINT DEFAULT '168' NOT NULL,
    ADD CONSTRAINT ck_fa_config_max_hold CHECK (max_hold_hours BETWEEN 1 AND 8760);
