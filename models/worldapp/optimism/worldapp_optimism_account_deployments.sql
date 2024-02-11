{{ config
(
    materialized = 'incremental',
    unique_key = ['TRANSACTION_HASH','TRACE_ID']
)
}}

SELECT 
BLOCK_TIMESTAMP,
date_trunc('day',BLOCK_TIMESTAMP) as BLOCK_DATE,
TRANSACTION_HASH,
TRACE_ID,
CONCAT('0x', SUBSTRING(OUTPUT, 27, 40)) AS ACCOUNT
FROM OPTIMISM.RAW.TRACES
WHERE FROM_ADDRESS IN (
    '0x9b3a6c86dbfa8842f860cc2f20e5fd95d98ad8cf',
    '0x8909aa707db02acae6ccde78f112aa2c92b5dded',
    '0x7861b53731f4f29108e31c2b38dbbbc754ae6453',
    '0xb4f9adb11ba5df95844d0c9eba6a7694690b3d44',
    '0x782ace3e8265d9ec065e46a9b779605ca11000bc',
    '0x730c20754f49b8a65458ebc9e826c3809970802a',
    '0x2dec4685caf5a8dc41018756361dbd952fbbf49c',
    '0xb391e960aec52f5fbdca9248751bb1cf5df57a53',
    '0x136762e94b1e4d4b13621f169b07207dac3fd0ce',
    '0x36e68d3f26d14b2b9bae2a83a1ce88a484d497cd',
    '0x91a3060513f25b44bacb46762160f6794bbd42d2',
    '0xe74e592e1dd43ccfb55bcd7999dee0dfee7acae3',
    '0x279a5453597e07505d574233fd16fbc670838fe7',
    '0x71730945b56a1472874edf3e40795d74ef350416',
    '0x85e38523a65a5c9f628c5477033c742da1c218cc',
    '0x36c334bd446ad62e602672828d21029ab48dd27f',
    '0x1d234d6d83b0535ce28d11bbbbbc9426868d4cb9',
    '0x9f4665f0ce1a377abdbd638f81f0c5883fbedcdb',
    '0x77db88c013d5149d994b17f6586cd71c3ffd2503',
    '0xc5a53b78734284d683d623c5c06d2e646abcec3f',
    '0xc0edd4902879a7e85b4bd2dfe293dbec4d838c2d'
)
AND SELECTOR = '0x1688f0b9'
AND ERROR IS NULL
{% if not is_incremental() %}
AND BLOCK_TIMESTAMP >= '2023-06-01'
{% endif %}
{% if is_incremental() %}
AND BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP() - interval '3 day' 
{% endif %}