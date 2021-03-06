import React, { useState } from "react";
import { FormattedMessage } from "react-intl";

import ContentCard from "../../../components/ContentCard";
import ServiceForm from "../../../components/ServiceForm";
import { AnalyticsService } from "../../../core/analytics/AnalyticsService";
import { Source } from "../../../core/resources/Source";

import { useSourceDefinitionSpecificationLoad } from "../../../components/hooks/services/useSourceHook";

import usePrepareDropdownLists from "./usePrepareDropdownLists";

import { IDataItem } from "../../../components/DropDown/components/ListItem";
import { createFormErrorMessage } from "../../../utils/errorStatusMessage";
import { JobInfo } from "../../../core/resources/Scheduler";
import { JobsLogItem } from "../../../components/JobItem";

type IProps = {
  source?: Source;
  onSubmit: (values: {
    name: string;
    serviceType: string;
    sourceDefinitionId?: string;
    connectionConfiguration?: any;
  }) => void;
  dropDownData: IDataItem[];
  hasSuccess?: boolean;
  errorStatus?: number;
  jobInfo?: JobInfo;
  afterSelectConnector?: () => void;
};

const SourceStep: React.FC<IProps> = ({
  onSubmit,
  dropDownData,
  hasSuccess,
  errorStatus,
  source,
  jobInfo,
  afterSelectConnector
}) => {
  const [sourceDefinitionId, setSourceDefinitionId] = useState(
    source?.sourceDefinitionId || ""
  );
  const {
    sourceDefinitionSpecification,
    isLoading
  } = useSourceDefinitionSpecificationLoad(sourceDefinitionId);

  const { getSourceDefinitionById } = usePrepareDropdownLists();

  const onDropDownSelect = (sourceId: string) => {
    const sourceDefinition = getSourceDefinitionById(sourceId);

    AnalyticsService.track("New Source - Action", {
      action: "Select a connector",
      connector_source: sourceDefinition?.name,
      connector_source_id: sourceDefinition?.sourceDefinitionId
    });

    if (afterSelectConnector) {
      afterSelectConnector();
    }

    setSourceDefinitionId(sourceId);
  };

  const onSubmitForm = async (values: { name: string; serviceType: string }) =>
    onSubmit({
      ...values,
      sourceDefinitionId: sourceDefinitionSpecification?.sourceDefinitionId
    });

  const errorMessage = createFormErrorMessage(errorStatus);

  return (
    <ContentCard title={<FormattedMessage id="onboarding.sourceSetUp" />}>
      <ServiceForm
        allowChangeConnector
        onDropDownSelect={onDropDownSelect}
        onSubmit={onSubmitForm}
        formType="source"
        dropDownData={dropDownData}
        hasSuccess={hasSuccess}
        errorMessage={errorMessage}
        specifications={sourceDefinitionSpecification?.connectionSpecification}
        documentationUrl={sourceDefinitionSpecification?.documentationUrl}
        isLoading={isLoading}
        formValues={source}
      />
      <JobsLogItem jobInfo={jobInfo} />
    </ContentCard>
  );
};

export default SourceStep;
