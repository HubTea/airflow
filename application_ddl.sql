-- api 서버에서 사용하게 될 테이블
create table company (
    id varchar(8) primary key, -- 기업 고유 코드

    debt_equity_ratio real not null, -- 부채-자본 비율
    return_on_equity real not null, -- 자기자본이익률
    highest_dividend_yield numeric(4, 2), -- 보통주와 우선주 중 높은 배당률
    
    name varchar(255) not null, -- 기업 이름
    liabilities bigint not null, -- 부채
    equity bigint not null, -- 자본
    assets bigint not null, -- 자산
    revenue bigint not null, -- 매출
    operating_income bigint not null, -- 영업이익
    net_income bigint not null, -- 당기순이익

    highest_dividend_type varchar(10) not null check (highest_dividend_type in ('common', 'preferred')),
    common_stock_yield numeric(4, 2) not null, -- 보통주 시가 배당률
    preferred_stock_yield numeric(4, 2) not null, -- 우선주 시가 배당률

    newly_crossed_debt_equity_ratio_threshold boolean not null, -- 이번 분기에 기준치를 하회했는지?
    newly_crossed_return_on_equity_threshold boolean not null, -- 이번 분기에 기준치를 상회했는지?
    newly_crossed_highest_dividend_yield boolean not null -- 이번 분기에 기준치를 상회했는지?
);

create index debt_equity_ratio on company (debt_equity_ratio);
create index return_on_equity on company (return_on_equity);
create index highest_dividend_yield on company (highest_dividend_yield);
