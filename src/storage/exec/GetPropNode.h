/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_GETPROPNODE_H_
#define STORAGE_EXEC_GETPROPNODE_H_

#include "common/base/Base.h"
#include "utils/ContainerConv.h"
#include "storage/exec/TagNode.h"
#include "storage/exec/EdgeNode.h"

namespace nebula {
namespace storage {

class GetVertexPropNode : public QueryNode<VertexID> {
public:
    explicit GetVertexPropNode(const PlanContext *context, DataSet *resultDataSet)
        : ctx_(context), resultDataSet_(resultDataSet) {}

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        auto ret = RelNode::execute(partId, vId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        std::unique_ptr<kvstore::KVIterator> iter;
        vertexPrefix_ = NebulaKeyUtils::vertexPrefix(ctx_->vIdLen_, partId, vId);
        ret = ctx_->env_->kvstore_->prefix(ctx_->spaceId_, partId, vertexPrefix_, &iter);
        if (ret == kvstore::ResultCode::SUCCEEDED && iter && iter->valid()) {
            iter_.reset(new SingleVertexIterator(std::move(iter)));
        } else {
            iter_.reset();
        }

        Row row;
        for (const auto &prop : resultDataSet_->colNames) {
            if (prop == "_tags") {
                row.emplace_back(collectTags());
            } else {
                DLOG(FATAL) << "Unkown vertex reserved properties `" << prop << "'.";
                row.emplace_back(Value::kEmpty);
            }
        }
        resultDataSet_->rows.emplace_back(std::move(row));

        return kvstore::ResultCode::SUCCEEDED;
    }

private:
    // List<TagID>
    List collectTags() {
        std::unordered_set<Value> tagIds;
        tagIds.reserve(32);
        for (; iter_ != nullptr && iter_->valid(); iter_->next()) {
            TagID tagId = NebulaKeyUtils::getTagId(ctx_->vIdLen_, iter_->val());
            // CommonUtils::checkDataExpiredForTTL(schemas_->back().get(), reader_.get(),
                                                // ttlValue.first, ttlValue.second)
            tagIds.emplace(tagId);
        }
        return List(ContainerConv::to<std::vector>(std::move(tagIds)));
    }

    const PlanContext                 *ctx_;
    std::unique_ptr<StorageIterator>   iter_;  // iterator for vertex
    nebula::DataSet                   *resultDataSet_;
    std::string                        vertexPrefix_;
    std::unordered_map<TagID, std::pair<std::string, int64_t>> tagsTTL_;
};

class GetTagPropNode : public QueryNode<VertexID> {
public:
    explicit GetTagPropNode(std::vector<TagNode*> tagNodes,
                            nebula::DataSet* resultDataSet)
        : tagNodes_(std::move(tagNodes))
        , resultDataSet_(resultDataSet) {}

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        auto ret = RelNode::execute(partId, vId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        std::vector<Value> row;
        // vertexId is the first column
        row.emplace_back(vId);
        for (auto* tagNode : tagNodes_) {
            const auto& tagName = tagNode->getTagName();
            ret = tagNode->collectTagPropsIfValid(
                [&row] (const std::vector<PropContext>* props) -> kvstore::ResultCode {
                    for (const auto& prop : *props) {
                        if (prop.returned_) {
                            row.emplace_back(NullType::__NULL__);
                        }
                    }
                    return kvstore::ResultCode::SUCCEEDED;
                },
                [this, &row, &tagName] (TagID tagId,
                                        RowReader* reader,
                                        const std::vector<PropContext>* props)
                -> kvstore::ResultCode {
                    nebula::List list;
                    auto code = collectTagProps(tagId,
                                                tagName,
                                                reader,
                                                props,
                                                list);
                    if (code != kvstore::ResultCode::SUCCEEDED) {
                        return code;
                    }
                    for (auto& col : list.values) {
                        row.emplace_back(std::move(col));
                    }
                    return kvstore::ResultCode::SUCCEEDED;
                });
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }
        }
        resultDataSet_->rows.emplace_back(std::move(row));
        return kvstore::ResultCode::SUCCEEDED;
    }

private:
    std::vector<TagNode*> tagNodes_;
    nebula::DataSet* resultDataSet_;
};

class GetEdgePropNode : public QueryNode<cpp2::EdgeKey> {
public:
    GetEdgePropNode(std::vector<EdgeNode<cpp2::EdgeKey>*> edgeNodes,
                    size_t vIdLen,
                    nebula::DataSet* resultDataSet)
        : edgeNodes_(std::move(edgeNodes))
        , vIdLen_(vIdLen)
        , resultDataSet_(resultDataSet) {}

    kvstore::ResultCode execute(PartitionID partId, const cpp2::EdgeKey& edgeKey) override {
        auto ret = RelNode::execute(partId, edgeKey);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        std::vector<Value> row;
        for (auto* edgeNode : edgeNodes_) {
            ret = edgeNode->collectEdgePropsIfValid(
                [&row] (const std::vector<PropContext>* props) -> kvstore::ResultCode {
                    for (const auto& prop : *props) {
                        if (prop.returned_) {
                            row.emplace_back(NullType::__NULL__);
                        }
                    }
                    return kvstore::ResultCode::SUCCEEDED;
                },
                [this, &row] (EdgeType edgeType,
                              folly::StringPiece key,
                              RowReader* reader,
                              const std::vector<PropContext>* props)
                -> kvstore::ResultCode {
                    nebula::List list;
                    auto code = collectEdgeProps(edgeType,
                                                 reader,
                                                 key,
                                                 vIdLen_,
                                                 props,
                                                 list);
                    if (code != kvstore::ResultCode::SUCCEEDED) {
                        return code;
                    }
                    for (auto& col : list.values) {
                        row.emplace_back(std::move(col));
                    }
                    return kvstore::ResultCode::SUCCEEDED;
                });
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }
        }
        resultDataSet_->rows.emplace_back(std::move(row));
        return kvstore::ResultCode::SUCCEEDED;
    }

private:
    std::vector<EdgeNode<cpp2::EdgeKey>*> edgeNodes_;
    size_t vIdLen_;
    nebula::DataSet* resultDataSet_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_GETPROPNODE_H_
