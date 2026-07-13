import pandas as pd
from query_script import QueryExecutor

def attach_values_to_origin(existing_tuple: tuple, join_column: str = 'eqp_id') -> tuple:
    """
    (DataFrame, ) 형태로 튜플 안에 들어있는 기존 오리진 데이터를 받아서,
    DB에서 조회한 설비별 3가지 값을 각 eqp_id에 맞게 매핑(Merge)한 뒤
    다시 튜플 형태로 반환하는 함수입니다.
    """
    # 1. 튜플에서 기존 오리진 DataFrame을 꺼냅니다.
    original_df = existing_tuple[0]
    
    # 2. DB에서 쿼리 실행 (설비별로 한 줄씩 정리된 피벗 DataFrame 반환됨)
    executor = QueryExecutor()
    new_values_df = executor.execute_query()
    
    # 3. 기존 오리진 DataFrame에 새로 조회한 값들을 옆에 착! 붙여줍니다.
    # original_df의 eqp_id 컬럼과 new_values_df의 인덱스(eqp_id)를 매칭시킵니다.
    final_df = original_df.merge(new_values_df, left_on=join_column, right_index=True, how='left')
    
    # 4. 기존 형태 그대로 다시 튜플에 담아서 돌려줍니다.
    return (final_df, )

if __name__ == "__main__":
    # --- [사용 예시] ---
    print("--- 튜플 안의 오리진 DataFrame에 매핑(Merge) 테스트 ---")
    
    # (가정) 기존에 받아오던 튜플 형태의 데이터
    my_old_df = pd.DataFrame({
        'eqp_id': ['EQP_001', 'EQP_002', 'EQP_003'],
        'status': ['RUN', 'STOP', 'IDLE']
    })
    my_existing_tuple = (my_old_df, )
    
    print("병합 전 원본 튜플 안의 df:")
    print(my_existing_tuple[0])
    print("-" * 30)
    
    # 함수에 튜플을 통째로 넣어서 결과 튜플을 반환받습니다.
    my_new_tuple = attach_values_to_origin(my_existing_tuple, join_column='eqp_id')
    
    print("병합 후 결과 튜플 안의 최종 df:")
    print(my_new_tuple[0])
