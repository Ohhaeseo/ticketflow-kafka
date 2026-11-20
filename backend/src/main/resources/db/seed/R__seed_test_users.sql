-- 로컬·테스트 전용 합성 사용자 1,000명 (id 1~1000). 실제 개인정보 없음 (PRD §2.2).
-- local 프로필에서만 이 위치(classpath:db/seed)를 Flyway에 추가한다.
-- 반복 실행해도 결과가 같도록 작성한다.
INSERT INTO users (id, nickname)
SELECT g, 'test-user-' || lpad(g::text, 4, '0')
  FROM generate_series(1, 1000) AS g
    ON CONFLICT (id) DO NOTHING;

SELECT setval(pg_get_serial_sequence('users', 'id'), GREATEST((SELECT max(id) FROM users), 1));
