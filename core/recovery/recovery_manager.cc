#include "recovery/recovery_manager.h"

#include <algorithm>
#include <vector>

#include <butil/logging.h>

namespace {
constexpr int kLogFileHeaderSize = sizeof(batch_id_t) + sizeof(size_t);
}

void RecoveryManager::PerformRecovery() {
	Analysis();
	Redo();
	Undo();
	Reset();
}

void RecoveryManager::Analysis() {
	page_update_logs_.clear();
	orphan_update_logs_.clear();
	loser_page_logs_.clear();
	committed_batches_.clear();

	if (disk_manager_ == nullptr) {
		LOG(WARNING) << "RecoveryManager Analysis skipped: disk manager is null";
		return;
	}

	int file_size = disk_manager_->get_file_size(LOG_FILE_NAME);
	if (file_size <= kLogFileHeaderSize) {
		LOG(INFO) << "RecoveryManager Analysis: log file has no payload";
		return;
	}

	int payload_bytes = file_size - kLogFileHeaderSize;
	std::vector<char> buffer(payload_bytes);
	int bytes_read = log_replay_.read_log(buffer.data(), payload_bytes, kLogFileHeaderSize);
	if (bytes_read <= 0) {
		LOG(WARNING) << "RecoveryManager Analysis: failed to read redo log payload";
		return;
	}

	int cursor = 0;
	while (cursor + OFFSET_LOG_TOT_LEN + static_cast<int>(sizeof(uint32_t)) <= bytes_read) {
		uint32_t record_size = *reinterpret_cast<const uint32_t*>(buffer.data() + cursor + OFFSET_LOG_TOT_LEN);
		if (record_size == 0) {
			LOG(WARNING) << "RecoveryManager Analysis: hit zero-sized log record";
			break;
		}
		if (cursor + static_cast<int>(record_size) > bytes_read) {
			LOG(WARNING) << "RecoveryManager Analysis: incomplete log record truncated at end of file";
			break;
		}

		LogType log_type = *reinterpret_cast<const LogType*>(buffer.data() + cursor + OFFSET_LOG_TYPE);
		switch (log_type) {
			case LogType::UPDATE: {
				auto update_log = std::make_unique<UpdateLogRecord>();
				update_log->deserialize(buffer.data() + cursor);
				page_update_logs_[update_log->rid_.page_no_].push_back(std::move(update_log));
				break;
			}
			case LogType::BATCHEND: {
				BatchEndLogRecord batch_end_log;
				batch_end_log.deserialize(buffer.data() + cursor);
				committed_batches_.insert(batch_end_log.log_batch_id_);
				break;
			}
			default:
				break;
		}

		cursor += record_size;
	}

	for (auto it = page_update_logs_.begin(); it != page_update_logs_.end();) {
		int page_id = it->first;
		auto &logs = it->second;
		if (logs.empty()) {
			it = page_update_logs_.erase(it);
			continue;
		}
		std::sort(logs.begin(), logs.end(), [](const auto &lhs, const auto &rhs) {
			return lhs->lsn_ < rhs->lsn_;
		});

		std::vector<std::unique_ptr<UpdateLogRecord>> committed_logs;
		std::vector<std::unique_ptr<UpdateLogRecord>> loser_logs;
		committed_logs.reserve(logs.size());
		for (auto &log_ptr : logs) {
			if (log_ptr == nullptr) {
				continue;
			}
			LLSN log_lsn = log_ptr->lsn_;
			if (committed_batches_.count(log_ptr->log_batch_id_) > 0) {
				committed_logs.push_back(std::move(log_ptr));
			} else {
				orphan_update_logs_[page_id].push_back(log_lsn);
				if (log_ptr->HasUndoPayload()) {
					loser_logs.push_back(std::move(log_ptr));
				} else {
					LOG(WARNING) << "Undo payload missing for LSN " << log_lsn << " on page " << page_id;
				}
			}
		}
		if (!loser_logs.empty()) {
			auto &loser_slot = loser_page_logs_[page_id];
			for (auto &entry : loser_logs) {
				loser_slot.push_back(std::move(entry));
			}
		}
		if (committed_logs.empty()) {
			it = page_update_logs_.erase(it);
		} else {
			logs = std::move(committed_logs);
			++it;
		}
	}
}

void RecoveryManager::Redo() {
	for (auto &entry : page_update_logs_) {
		int page_id = entry.first;
		LLSN checkpoint_lsn = 0;
		auto ckpt_it = checkpoint.find(page_id);
		if (ckpt_it != checkpoint.end()) {
			checkpoint_lsn = ckpt_it->second;
		}

		for (auto &log_ptr : entry.second) {
			if (log_ptr == nullptr) {
				continue;
			}
			if (log_ptr->lsn_ <= checkpoint_lsn) {
				continue;
			}
			log_replay_.apply_sigle_log(log_ptr.get(), 0);
			checkpoint_lsn = log_ptr->lsn_;
		}

		checkpoint[page_id] = checkpoint_lsn;
	}
}

void RecoveryManager::Undo() {
	for (auto &entry : loser_page_logs_) {
		auto &logs = entry.second;
		if (logs.empty()) {
			continue;
		}
		std::sort(logs.begin(), logs.end(), [](const auto &lhs, const auto &rhs) {
			return lhs->lsn_ > rhs->lsn_;
		});
		for (auto &log_ptr : logs) {
			if (log_ptr == nullptr) {
				continue;
			}
			if (!log_ptr->HasUndoPayload()) {
				LOG(WARNING) << "Skip undo for LSN " << log_ptr->lsn_ << " due to missing before-image";
				continue;
			}
			log_replay_.apply_undo_log(log_ptr.get());
		}
	}
}

void RecoveryManager::Reset() {
	page_update_logs_.clear();
	orphan_update_logs_.clear();
	loser_page_logs_.clear();
	committed_batches_.clear();
}
