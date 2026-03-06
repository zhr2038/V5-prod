# ML 最终形态训练门禁（生产版）

> 目标：模型可以持续学习，但不允许“不稳模型”覆盖线上版本。

## 流程（最终形态）

1. 导出训练集（`reports/ml_training_data.csv`）
2. 动态特征选择（`optimize_features_for_training`）
3. 白名单兜底（动态不稳时自动回退固定特征）
4. 时序CV评估（`TimeSeriesSplit` + IC）
5. Holdout评估（train/valid IC）
6. **双闸门判定**
   - `valid_ic >= V5_ML_MIN_VALID_IC`
   - `ic_gap <= V5_ML_MAX_IC_GAP`
   - `cv_mean_ic >= V5_ML_MIN_CV_MEAN_IC`
   - `cv_std_ic <= V5_ML_MAX_CV_STD`
7. 通过才覆盖模型；不通过则保留旧模型
8. 全量记录到 `reports/ml_training_history.json`

## 当前生产参数（systemd 环境变量）

`/home/admin/.config/systemd/user/v5-daily-ml-training.service`

- `V5_ML_MIN_SAMPLES=200`
- `V5_ML_MIN_VALID_IC=-0.10`
- `V5_ML_MAX_IC_GAP=0.90`
- `V5_ML_MIN_CV_MEAN_IC=0.00`
- `V5_ML_MAX_CV_STD=0.35`

## 结果语义

- `saved`：新模型通过门禁，已覆盖
- `blocked`：门禁拦截，旧模型保留
- `insufficient`：数据不足，未训练
- `error`：训练链路异常

> 注意：`blocked/insufficient` 视为流程成功完成（退出码0），避免误报警。
