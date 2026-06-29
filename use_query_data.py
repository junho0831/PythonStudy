import pandas as pd
from query_script import QueryExecutor

def process_equipment_data(target_eqp: str):
    """
    (이전 예시) 특정 설비(target_eqp)의 값만 추출하여 변수로 사용하는 함수.
    """
    # 1. 쿼리 실행 (이제 DataFrame이 반환됨)
    executor = QueryExecutor()
    equipment_df = executor.execute_query()

    # 2. DataFrame에서 원하는 설비의 값만 추출
    if target_eqp in equipment_df.index:
        corner_length_valn = equipment_df.loc[target_eqp, 'corner_length']
        corner_width_valn  = equipment_df.loc[target_eqp, 'corner_width']
        border_width_valn  = equipment_df.loc[target_eqp, 'border_width']
        
        return corner_length_valn, corner_width_valn, border_width_valn
    else:
        return None, None, None

def merge_equipment_data_to_df(original_df: pd.DataFrame, join_column: str = 'eqp_id') -> pd.DataFrame:
    """
    기존에 가지고 있던 DataFrame을 인자로 받아서, 
    DB에서 쿼리한 결과를 새로운 컬럼으로 병합(Merge)하여 반환하는 함수입니다.
    """
    # 1. 쿼리 실행 (이제 딕셔너리가 아니라 이미 피벗이 완료된 깔끔한 DataFrame이 나옵니다!)
    executor = QueryExecutor()
    new_values_df = executor.execute_query()
    
    # 2. 기존 DataFrame(original_df)과 새 DataFrame(new_values_df) 병합하기
    # new_values_df의 인덱스는 'eqp_id'로 되어 있으므로 right_index=True를 씁니다.
    final_df = original_df.merge(new_values_df, left_on=join_column, right_index=True, how='left')
    
    return final_df

if __name__ == "__main__":
    # --- [사용 예시 1] 변수만 뽑아 쓰기 ---
    print("--- [사용 예시 1] 단일 설비 변수 추출 ---")
    length, width, border = process_equipment_data('EQP_001')
    print(f"EQP_001 값: {length}, {width}, {border}\n")
    
    # --- [사용 예시 2] 기존 DataFrame에 붙여넣기 ---
    print("--- [사용 예시 2] 기존 DataFrame에 병합(Merge) ---")
    # (가정) 원래 다른 곳에서 쓰던 기존 DataFrame 만들기
    my_old_df = pd.DataFrame({
        'eqp_id': ['EQP_001', 'EQP_002', 'EQP_003'],
        'status': ['RUN', 'STOP', 'IDLE']
    })
    print("병합 전 원본 df:")
    print(my_old_df)
    print("-" * 30)
    
    # 함수에 기존 df를 넣어서 병합된 새 df를 반환받습니다.
    my_new_df = merge_equipment_data_to_df(my_old_df, join_column='eqp_id')
    print("병합 후 최종 df:")
    print(my_new_df)
