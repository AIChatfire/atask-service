# 要优化的点

## 适配 minimax-h3 ✅ 已完成（2026-08-12）
- 接入方式：`POST /minimax/v1/videos`（或 `/minimax/v1/tasks`），请求体与下方
  示例完全一致（content[] 多模态结构原样透传，t2va/i2va/r2va 零差异支持）。
- 网关零代码适配：渠道挂到统一分组（默认 `keypool`），网关配置放
  `header_override.upstream` 嵌套块（与 `setting.gateway` 等价）：
  `biz=minimax` + submit_path=/v2/video_generation、
  probe_path=/v2/query/video_generation/{upstream_task_id}、
  status_path=task.status、result_path=task.content.url、
  settle_usage_map={duration: task.usage.output_seconds}。
- 验收：`tests/test_minimax_e2e.py`（提交→探测→重估结算→回调通知全链路）。
- 真实上游实测（2026-08-13，metaso.cn 代理）：创建 `{"task_id":...}`、查询
  `task.status`（queued→running→succeeded）、`task.content.url`（video/mp4
  可下载）、`task.usage.output_seconds` 与网关提取配置逐字段吻合；402 错误
  信封（`error.message`）由网关映射为任务失败 + 解冻 + key 上报。
- [接口文档](https://platform.minimaxi.com/docs/api-reference/video-generation-v2-create)
- 示例
    ```shell
    # t2va
    curl --request POST \
      --url https://metaso.cn/api/minimax/v2/video_generation \
      --header 'Authorization: Bearer mk-094B26111DEC2372A135C278A2D44270' \
      --header 'Content-Type: application/json' \
      --data '
    {
      "model": "MiniMax-H3",
      "content": [
        {
          "type": "text",
          "text": "史诗级太空歌剧院线预告：女舰长独自站在巨大观景窗前，最后一支舰队正在集结并跃迁离去，强光爆闪、舰桥震动，她被留在原地。"
        }
      ],
      "resolution": "2K",
      "duration": 5,
      "ratio": "16:9"
    }
    '
    
    # i2va
    curl --request POST \
      --url https://metaso.cn/api/minimax/v2/video_generation \
      --header 'Authorization: Bearer mk-094B26111DEC2372A135C278A2D44270' \
      --header 'Content-Type: application/json' \
      --data '
    {
      "model": "MiniMax-H3",
      "content": [
        {
          "type": "text",
          "text": "Pull focus to the people in the background and add more steam to the ramen bowl."
        },
        {
          "type": "image_url",
          "image_url": {
            "url": "https://cdn.hailuoai.com/prod/hailuo_demo/testsets/H3_AA_I2VA/gallery/sr_v17_variants_seed42_43_20260724/inputs/4a3a90bf9100_KDmcbkhzYo5sjjxr9FqcVmWVnzb.png"
          },
          "role": "first_frame"
        }
      ],
      "resolution": "2K",
      "duration": 5,
      "ratio": "adaptive"
    }
    '
    
    # r2va
    curl --request POST \
      --url https://metaso.cn/api/minimax/v2/video_generation \
      --header 'Authorization: Bearer mk-094B26111DEC2372A135C278A2D44270' \
      --header 'Content-Type: application/json' \
      --data '
    {
      "model": "MiniMax-H3",
      "content": [
        {
          "type": "text",
          "text": "角色说话：Follow the wind, live free.Leave worries behind, enjoy the moment，音色参考音频1"
        },
        {
          "type": "video_url",
          "video_url": {
            "url": "https://cdn.hailuoai.com/prod/hailuo_demo/testsets/h3_promo_eval_ref2va/gallery/sr_v2p26_trio_seed42_20260724/inputs/297573323635_00_%E8%A7%86%E9%A2%911_YnyRbxEwio_video_20260525_163755_1927e9d3.mp4"
          },
          "role": "reference_video"
        },
        {
          "type": "audio_url",
          "audio_url": {
            "url": "https://cdn.hailuoai.com/prod/hailuo_demo/testsets/h3_promo_eval_ref2va/gallery/sr_v2p26_trio_seed42_20260724/inputs/f463d523c5ce_01_%E9%9F%B3%E9%A2%911_RSLcbpzJPo_6%E6%9C%885%E6%97%A5(1).mp3"
          },
          "role": "reference_audio"
        }
      ],
      "resolution": "2K",
      "duration": 5,
      "ratio": "adaptive"
    }
    '
    ```