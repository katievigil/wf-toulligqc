
// Developer notes
//
// This template workflow provides a basic structure to copy in order
// to create a new workflow. Current recommended practices are:
//     i) create a simple command-line interface.
//    ii) include an abstract workflow scope named "pipeline" to be used
//        in a module fashion
//   iii) a second concrete, but anonymous, workflow scope to be used
//        as an entry point when using this workflow in isolation.

import groovy.json.JsonBuilder
nextflow.enable.dsl = 2

OPTIONAL_FILE = file("$projectDir/data/OPTIONAL_FILE")

process getVersions {
    label "wfqc"
    cpus 1
    output:
        path "versions.txt"
    script:
    """
    toulligqc --version | sed 's/^/toulligQC,/' >> versions.txt
    """
}

process getParams {
    label "wfqc"
    cpus 1
    output:
        path "params.json"
    script:
        String paramsJSON = new JsonBuilder(params).toPrettyString()
    """
    # Output nextflow params object to JSON
    echo '$paramsJSON' > params.json
    """
}

process makeReport {
    label "wfqc"
    input:
        path plots_dir
        path "QC-repport/report.data"
        path "versions/*"
        path "params.json"
    output:
        path "wf-toulligqc-*.html"
    script:
        String report_name = "wf-toulligqc-report.html"
    """
    workflow-glue report $report_name \
        --versions versions \
        --params params.json \
        --metadata QC-repport/report.data \
        --qc ${plots_dir}
    """
}

process toulligqc {
    //debug true 
    label "wfqc"
    input:
        path seq_summary
        path summary_pass
        path summary_fail
        path seq_telemetry
        path fast5
        path bam
        path fastq
        val report_name
        val barcodes
        val barcoding
    output:
        path "$report_name/report.data", emit: report_data
        path "$report_name/images/*.html", emit: plots_html
        path "$report_name/images/plotly.min.js", emit: plotly_js
        path "$report_name/images", emit: plots_dir
    script:
        def seq_summary_arg = seq_summary.name != 'no_seq_summary' ? "--sequencing-summary-source $seq_summary" : ""
        def summary_pass_arg = summary_pass.name != 'no_barcoding_pass' ? "--sequencing-summary-source $summary_pass" : ""
        def summary_fail_arg = summary_fail.name != 'no_barcoding_fail' ? "--sequencing-summary-source $summary_fail" : ""
        def telemetry_arg = seq_telemetry.name != 'no_telemetry' ? "--telemetry-source $seq_telemetry" : ""
        def fast5_arg = fast5.name != 'no_fast5' ? "--fast5-source $fast5" : ""
        def bam_arg = bam.name != 'no_bam' ? "--bam $bam" : ""
        def fastq_arg = fastq.name != 'no_fastq' ? "--fastq $fastq" : ""
        def barcodes_list = barcodes != 'no_barcodes' ? "--barcodes $barcodes" : ""
        def barcoding_arg = barcoding != 'no_barcoding' ? "--barcoding" : ""
    """
    toulligqc $seq_summary_arg \
    $summary_pass_arg  $summary_fail_arg \
    $telemetry_arg  \
    $fast5_arg $fastq_arg $bam_arg\
    $barcoding_arg  $barcodes_list \
    -n $report_name \
    --force 
    """
} 

process output {
    label "wfqc"
    publishDir (
        params.out_dir,
        mode: "copy",
        saveAs: { dirname ? "$dirname/$fname" : fname }
    )
    input:
        tuple path(fname), val(dirname)
    output:
        path fname
    """
    """
}

workflow pipeline {
    take:
        seq_summary
        summary_pass
        summary_fail
        seq_telemetry
        fast5
        bam
        fastq
        report_name
        barcodes
        barcoding
    main:
        software_versions = getVersions()

        workflow_params = getParams()

        toulligqc(seq_summary, summary_pass, summary_fail, seq_telemetry, fast5, fastq, bam, report_name, barcodes, barcoding)

        plotly_js = toulligqc.out.plotly_js

        plots_dir = toulligqc.out.plots_dir

        report = makeReport(
            plots_dir, toulligqc.out.report_data, software_versions.collect(), workflow_params
        )
    emit:
        plotly_js
        report
        workflow_params
        telemetry = workflow_params
}

// entrypoint workflow
workflow {

    // EPI2ME safe guard
    if( !params.sequencing_summary_source &&
        !params.telemetry_source &&
        !params.fast5_source &&
        !params.fastq_source &&
        !params.bam_source ) {

        log.info "No input data provided — skipping pipeline execution (EPI2ME validation mode)"
        return
    }

    seq_summary = params.sequencing_summary_source ? file(params.sequencing_summary_source) : file("no_seq_summary")
    summary_pass = params.barcoding_summary_pass ? file(params.barcoding_summary_pass) : file("no_barcoding_pass")
    summary_fail = params.barcoding_summary_fail ? file(params.barcoding_summary_fail) : file("no_barcoding_fail")
    seq_telemetry = params.telemetry_source ? file(params.telemetry_source) : file("no_telemetry")
    fast5 = params.fast5_source ? file(params.fast5_source) : file("no_fast5")
    fastq = params.fastq_source ? file(params.fastq_source) : file("no_fastq")
    bam = params.bam_source ? file(params.bam_source) : file("no_bam")

    barcodes = params.barcodes ?: "no_barcodes"
    barcoding = params.barcoding ?: "no_barcoding"
    report_name = params.report_name ?: "ToulligQC_report"

    pipeline(
        seq_summary,
        summary_pass,
        summary_fail,
        seq_telemetry,
        fast5,
        bam,
        fastq,
        report_name,
        barcodes,
        barcoding
    )
}
