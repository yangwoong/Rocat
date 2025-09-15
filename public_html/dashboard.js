
/**
* 수질 데이터 JSON 객체를 생성하는 함수
* @param {Object} params - 수질 데이터 파라미터들
* @returns {Object} JSON 객체
*/
function createWaterQualityJSON(params) {
    const {
        mission_idx,
        water_q_idx,
        zone_id,
        lat,
        lon,
        curr_wq_state,
        target_wq_state,
        w_data,
        curr_windspeed,
        fc_windspeed,
        curr_fall,
        fc_fall,
        curr_datetime,
    } = params;

    return {
        mission_idx: mission_idx,
        water_q_idx: water_q_idx,
        zone_id: zone_id,
        lat: lat,
        lon: lon,
        curr_wq_state: curr_wq_state,
        target_wq_state: target_wq_state,
        w_data: {
            temp_c: w_data?.temp_c,
            ph: w_data?.ph,
            ec_us_cm: w_data?.ec_us_cm,
            do_mg_l: w_data?.do_mg_l,
            toc_mg_l: w_data?.toc_mg_l,
            cod_mg_l: w_data?.cod_mg_l,
            t_n_mg_l: w_data?.t_n_mg_l,
            t_p_mg_l: w_data?.t_p_mg_l,
            ss_mg_l: w_data?.ss_mg_l,
            cl_mg_l: w_data?.cl_mg_l,
            chl_a_mg_m3: w_data?.chl_a_mg_m3,
            cd_mg_l: w_data?.cd_mg_l,
            bod_mg_l: w_data?.bod_mg_l
        },
        curr_windspeed: curr_windspeed,
        fc_windspeed: fc_windspeed,
        curr_fall: curr_fall,
        fc_fall: fc_fall,
        curr_datetime: curr_datetime
    };
}



class IdGenerator {
    constructor() {
        this.counter = 0;
        this.prefix = 'MISSION';
    }

    generate() {
        this.counter++;
        const timestamp = Date.now();
        return `${this.prefix}_${timestamp}_${this.counter.toString().padStart(6, '0')}`;
    }

    // 날짜별 리셋
    generateDaily() {
        const today = new Date().toISOString().split('T')[0].replace(/-/g, '');
        this.counter++;
        return `${this.prefix}_${today}_${this.counter.toString().padStart(4, '0')}`;
    }
}
// const generator = new IdGenerator();
// console.log(generator.generate()); // "MISSION_1693456789123_000001"