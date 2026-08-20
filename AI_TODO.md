# 要优化的点

## keypool 精确直达 + 产物转存 ✅ 已完成（2026-08-20）
- 任务级操作（探测/取消/原生查询/回调/对账）用 `channel_id + key_index`
  单 key 精确直达（`app/services/leasing.py` 统一收口），根治同渠道多账号
  key 查不到任务的隐患；key 级失败自动降级渠道直达。
- 产物转存：渠道配 `result_url_template`（如
  `https://myhost.com/{upstream_result_url}`），上游直链在任务视图/回调/
  原生报文中统一改写为镜像链，原始直链另存 `data.upstream_result`。
- 验收：`tests/test_leasing.py` + `tests/test_resulturl.py`；
  详细清单见 OPTIMIZATION_BACKLOG.md。

## 原生接口同构（request/response 与上游一致，任务 id 用本地）✅ 已完成（2026-08-20）
- `POST /{biz}/v2/video_generation`（命中渠道 `submit_path`）不再同步透传：
  走 `flow.create_task` 异步受理，**请求内零上游往返**，响应
  `{"task_id": "minimax_<uuid4hex>"}`（形状由渠道 `task_id_path` + `ok_check`
  驱动，与上游报文严格同构）。
- `GET /{biz}/v2/query/video_generation/{id}`（命中 `probe_path`）：本地 id 与
  上游 id 都认，按 tasks 行的 `channel_id` 钉回直达租约转发探测，响应里的上游
  id 被逐字节改写回本地 id；worker 还没提交时按本地快照直出（200，不 404）。
- `cancel_path` 命中 → 本地 cancel（解冻 + 尽力源头止损），不当新任务计费。
- 免费 GET 透传不再对 keypool 发空 model 的 `select`（必拒 40010）。
- 验收：`tests/test_native_passthrough.py`；详细清单见 OPTIMIZATION_BACKLOG.md。

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