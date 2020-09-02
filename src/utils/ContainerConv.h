/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTILS_CONTAINERCONV_H_
#define UTILS_CONTAINERCONV_H_

#include "common/base/Base.h"

namespace nebula {

class ContainerConv {
public:
    explicit ContainerConv(...) = delete;

    template <template <typename, typename...> class To,
              typename T,
              template <typename, typename...> class From>
    static To<T> to(From<T> &&from) {
        To<T> to;
        to.reserve(from.size());
        to.insert(to.end(),
                  std::make_move_iterator(from.begin()),
                  std::make_move_iterator(from.end()));
        return to;
    }
};

}  // namespace nebula

#endif  // UTILS_CONTAINERCONV_H_
