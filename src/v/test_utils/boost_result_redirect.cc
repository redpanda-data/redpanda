// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "absl/strings/str_format.h"
#include "absl/strings/str_replace.h"
#include "absl/time/time.h"

#include <boost/test/results_collector.hpp>
#include <boost/test/tree/traverse.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/test/unit_test_suite.hpp>
#include <libxml/xmlwriter.h>

#include <stdexcept>

namespace {

class xml_writer {
public:
    explicit xml_writer(const std::string& path)
      : _writer(xmlNewTextWriterFilename(path.c_str(), 0)) {
        if (!_writer) {
            throw std::runtime_error("unable to create writer");
        }
        if (
          xmlTextWriterStartDocument(_writer, nullptr, "UTF-8", nullptr) < 0) {
            throw std::runtime_error("unable to start doc");
        }
        if (xmlTextWriterSetIndent(_writer, 2) < 0) {
            throw std::runtime_error("unable to use indent for doc");
        }
    }
    xml_writer(const xml_writer&) = delete;
    xml_writer(xml_writer&&) = delete;
    xml_writer& operator=(const xml_writer&) = delete;
    xml_writer& operator=(xml_writer&&) = delete;
    ~xml_writer() {
        if (_writer) {
            xmlFreeTextWriter(_writer);
        }
    }

    void start_element(const std::string& name) {
        int res = xmlTextWriterStartElement(_writer, BAD_CAST name.c_str());
        if (res < 0) {
            throw std::runtime_error("unable to start element");
        }
        ++_unclosed_elements;
    }
    void add_attr(const std::string& name, size_t value) {
        add_attr(name, absl::StrFormat("%d", value));
    }
    void add_attr(const std::string& name, double value) {
        // use fixed format to avoid scientific notation for
        // https://github.com/bazelbuild/bazel/issues/24605
        add_attr(name, absl::StrFormat("%.6f", value));
    }
    void
    add_attr(const std::string& name, boost::unit_test::const_string value) {
        add_attr(name, std::string{value.begin(), value.size()});
    }
    void add_attr(const std::string& name, const std::string& value) {
        int res = xmlTextWriterWriteAttribute(
          _writer, BAD_CAST name.c_str(), BAD_CAST value.c_str());
        if (res < 0) {
            throw std::runtime_error("unable to write attribute");
        }
    }
    void end_element() {
        int res = xmlTextWriterEndElement(_writer);
        if (res < 0) {
            throw std::runtime_error("unable to end element");
        }
        --_unclosed_elements;
    }

    void close() {
        for (int i = 0; i < _unclosed_elements; ++i) {
            end_element();
        }
        if (xmlTextWriterEndDocument(_writer) < 0) {
            throw std::runtime_error("unable to end doc");
        }
    }

private:
    int _unclosed_elements = 0;
    xmlTextWriterPtr _writer;
};

using namespace boost::unit_test;

/**
 * This test configuration object is used to redirect the JUNIT result output
 * (if enabled) to the location specified by the XML_OUTPUT_FILE environment
 * variable. This variable is set by the bazel test runner so that the test
 * writes the results in a local that bazel can subsequently consume.
 * Ref: CORE-8013.
 */
class bazel_test_observer : public test_observer {
    class reporter : public test_tree_visitor {
    public:
        explicit reporter(xml_writer* writer)
          : _writer(writer) {}

        void visit(const test_case& tc) override {
            const auto& results = results_collector.results(tc.p_id);
            _writer->start_element("testcase");
            const auto& suite = framework::get(
              tc.p_parent_id, test_unit_type::TUT_SUITE);
            _writer->add_attr(
              "classname", absl::StrReplaceAll(suite.full_name(), {{" ", ""}}));
            _writer->add_attr("name", tc.p_name);
            _writer->add_attr("file", tc.p_file_name);
            _writer->add_attr("line", tc.p_line_num);
            _writer->add_attr(
              "time",
              absl::ToDoubleSeconds(
                absl::Microseconds(results.p_duration_microseconds.get())));
            if (results.aborted()) {
                _writer->start_element("error");
                _writer->end_element();
            } else if (results.skipped()) {
                _writer->start_element("skipped");
                _writer->end_element();
            } else if (!results.passed()) {
                _writer->start_element("failure");
                _writer->end_element();
            }
            _writer->end_element();
        }
        bool test_suite_start(const test_suite& suite) override {
            _writer->start_element("testsuite");
            _writer->add_attr(
              "name", absl::StrReplaceAll(suite.p_name.get(), {{" ", ""}}));
            const auto& results = results_collector.results(suite.p_id);
            _writer->add_attr(
              "tests",
              results.p_test_cases_passed + results.p_test_cases_failed
                + results.p_test_cases_aborted + results.p_test_cases_skipped);
            _writer->add_attr("failures", results.p_test_cases_failed);
            _writer->add_attr("errors", results.p_test_cases_aborted);
            _writer->add_attr("skipped", results.p_test_cases_skipped);
            _writer->add_attr(
              "time",
              absl::ToDoubleSeconds(
                absl::Microseconds(results.p_duration_microseconds.get())));
            return true;
        }
        void test_suite_finish(const test_suite&) override {
            _writer->end_element();
        }

    private:
        xml_writer* _writer;
    };

public:
    bazel_test_observer() { framework::register_observer(*this); }
    bazel_test_observer(const bazel_test_observer&) = delete;
    bazel_test_observer(bazel_test_observer&&) = delete;
    bazel_test_observer& operator=(const bazel_test_observer&) = delete;
    bazel_test_observer& operator=(bazel_test_observer&&) = delete;
    ~bazel_test_observer() override { framework::deregister_observer(*this); }
    void test_finish() override { generate_output_file(); }
    void test_aborted() override { generate_output_file(); }
    int priority() override {
        // Just needs to be higher than 3, the test collector priority.
        constexpr int my_priority = 10;
        return my_priority;
    }

private:
    void generate_output_file() {
        if (_done) {
            return;
        }
        _done = true;
        if (auto xml_path = std::getenv("XML_OUTPUT_FILE")) {
            xml_writer w(xml_path);
            w.start_element("testsuites");
            reporter r{&w};
            traverse_test_tree(framework::master_test_suite(), r);
            w.end_element();
            w.close();
        }
    }

    bool _done = false;
};

} // namespace

BOOST_TEST_GLOBAL_CONFIGURATION(bazel_test_observer);
