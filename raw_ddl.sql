-- 기업의 가공하지 않은 JSON 형식의 핵심 재무정보와 배당 정보
create table raw_company (
    id varchar(8), -- 기업 고유 코드
    year int, -- 연도
    quarter int, -- 분기
    primary key (id, year, quarter),

    financial text not null,
    dividend text not null
);
