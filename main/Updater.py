import requests
import json
from tqdm import tqdm
from multiprocessing.dummy import Pool as ThreadPool
import time, os, sys
import yaml

from Updater_wrapper import *

from dotenv import load_dotenv
dotenv_path = '../Config/.env'
load_dotenv(dotenv_path)


#Snapshot: 

#Block height applied: 2477915

#Data (Sender, Receiver, Amount)

#5b4a4d4b9e64258ef438b5cb446434f3694f360fa0047bb819832ae1c0e86317

#3fc9cf92d1aad70702d54a89defd7abd54a5d3d414133e3bbf45f11011fb15a0

#Snapshot: 

#963f6f8febd29a2251d52801c4270f88b968db554d5676194c4f78ca1ce79c24


if __name__=='__main__':
    # use which full-node and ord server. 
    # If you have your own server, change it to 
    url_base = 'https://ordinalslite.com'

    #如果还没有同步过区块就选UPDATE_BLK_BY_BLK（逐块更新），如果之前同步过就选FAST_CATCHUP（快速追赶）会从下面设置的高度继续逐块更新
    mode = 'UPDATE_BLK_BY_BLK'

    # 快速追赶的高度设置
    fast_catchup_target_height = 2467872

    #backup tables parameters
    max_num_old_table = 5
    history_freq = 1000

    '''------------------------------------------------------------------
    ------------------------------------------------------------------
    in most of time, you do not need to change the following code
    ------------------------------------------------------------------
    ------------------------------------------------------------------'''
    #init the db
    # 设置最初索引区块，并以此初始化数据库
    ele = {'last_ins_num': 224060, 'last_height': 2465225}
    #初始化数据库
    init_idx_database(ele)

    # 链接初始化后的数据库
    db_manager = generate_a_db_manager()
    db_name = os.getenv('DB_NAME')
    db_manager.connect_with_postgres(db_name)

    #导入安东尼快照:
    snapshots_file = os.getenv('SP_FILE')
    with open(snapshots_file, 'r') as file:
        snapshots_details = yaml.safe_load(file)
    
    # 获取数据库索引中现在索引到的高度
    row = db_manager.search_a_table_with_constraints(db_manager.conn, 'misc', {'id': 1})
    last_ins_num = row[0][1]
    last_height = row[0][2]
    print('The last ins num in the db (included) is {}.'.format(last_ins_num))
    print('The last height in the db (included) is {}.'.format(last_height))


    # 获取目前的最近区块高度和索引号
    current_height = get_current_height(url_base)
    current_ins_num = get_current_ins_num(url_base)
    print('The current height and ins_num is {} and {}.'.format(current_height, current_ins_num))

    #下载快照并存储在数据库
    get_snapshots_data_until_target_height_seq(url_base, db_manager, snapshots_details, True, None)

    # 如果是快速追赶模式
    if mode == 'FAST_CATCHUP':
        if fast_catchup_target_height < last_height:
            #数据库数据不完整继续逐块更新
            print('*****BE CAREFUL**** we set the mode as UPDATE_BLK_BY_BLK')
            mode == 'UPDATE_BLK_BY_BLK'
        else:
            #download data. #we will only modify ltc20_ins_list and utxo_spent_list
            #通过数据库最新铭文编号查询铭文列表，并将未归纳铭文详情存储到数据库中
            get_ins_data_until_target_height_all_seq(url_base, db_manager, last_ins_num, 
                                                        last_height, fast_catchup_target_height) 
            #查询指定高度的所有交易对并将未归纳交易对存储在数据库
            get_txpairs_until_target_height_seq(url_base, db_manager, last_height, 
                                                        fast_catchup_target_height)

    continue_flag = True
    s = time.time()
    blk_num = 0
    while continue_flag:
        ss = time.time()

        #依据数据库区块高度切换模式
        if last_height > fast_catchup_target_height and mode == 'FAST_CATCHUP':
            mode = 'UPDATE_BLK_BY_BLK'
        
        #更新最新高度，如果相差为0一直等待
        current_height = get_current_height(url_base)
        remain = current_height - last_height
        while remain == 0:
            time.sleep(30)
            current_height = get_current_height(url_base)
            remain = current_height - last_height

        print('Current height {} and we are at {}. {} blocks remain.'.format(current_height, last_height, remain))
        es = time.time()
        print('{} blocks finished. {} sec consumes in total'.format(blk_num, es-s))

        #get the data for the next block
        height = last_height + 1

        # get ranked tx in this block
        # ranked tx includes both the paired input/output spent AND the newly inscribed inscription
        # 获取该区块中交易的排名
        # 排名区块中的 tx 包括花费的成对输入/输出和新刻入的铭文
        all_tx_value, last_ins_num_block = get_ranked_txpair_and_ins_at_a_height(url_base, db_manager, height, snapshots_details, last_ins_num, mode)
        print(all_tx_value)
        print(last_ins_num_block)

        # modify the balance according to the ranked tx
        # 根据排名后的tx修改余额
        for a in all_tx_value:
            if a[3]['type']=='ins':
                modify_balance_by_new_inscribed_inscription(db_manager, a[3]['data'])
            if a[3]['type']=='txpair':
                valid_spent = modify_balance_according_to_a_tx_pair(db_manager, a[3]['data'], snapshots_details)

        #move to the next block
        last_height = height
        last_ins_num = last_ins_num_block
        blk_num = blk_num + 1

        # update the current position
        # 更新最新的数据库铭文编号和高度
        ele = {'last_ins_num': last_ins_num, 'last_height': last_height}
        row = db_manager.update_a_row_with_constraint(db_manager.conn, 'misc', ele, {'id': 1})

        # take a snapshot
        #take_a_history_checkpoint(db_manager, height, max_num_old_table, history_freq)

        ee = time.time()

        print('Spend: {} sec for the current block'.format(ee-ss))
        print('======================================')

        time.sleep(100)

    db_manager.conn.close()
