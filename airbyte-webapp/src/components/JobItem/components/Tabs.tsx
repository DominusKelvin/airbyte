import React from "react";
import styled from "styled-components";

import StepsMenu from "../../StepsMenu";

type IProps = {
  isFailed?: boolean;
  activeStep?: string;
  setAttemptNumber?: (id: string) => void;
  onSelect?: (id: string) => void;
  data: {
    id: string;
    name: string | React.ReactNode;
    status?: string;
    onSelect?: () => void;
  }[];
};

const TabsContent = styled.div<{ isFailed?: boolean }>`
  padding: 6px 0;
  border-bottom: 1px solid
    ${({ theme, isFailed }) =>
      isFailed ? theme.dangerTransparentColor : theme.greyColor20};
`;

const Tabs: React.FC<IProps> = ({
  isFailed,
  activeStep,
  setAttemptNumber,
  data
}) => {
  return (
    <TabsContent isFailed={isFailed}>
      <StepsMenu
        lightMode
        activeStep={activeStep}
        onSelect={setAttemptNumber}
        data={data}
      />
    </TabsContent>
  );
};

export default Tabs;
